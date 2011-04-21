require 'memcache'
require 'msgpack'
require 'logger'
require 'ruby-debug'
srand

class AbortException < Exception
end
class ActiveRetry < Exception
end
class NotInTransaction < Exception
end
class RetryException < Exception
end
class AlreadyOwn < Exception
end
class NotFoundException < Exception
end

$log=Logger.new(STDOUT)
$log.level=Logger::DEBUG
$log.level=Logger::INFO
$log.level=Logger::FATAL

$log.datetime_format=''


class MemcacheWrap
  def initialize(hostname)
    @client = MemCache.new(hostname)
    @lock = Mutex.new
  end
  def cas(key,&proc)
    $log.debug('begin to cas on ' + key)
    begin
      $log.debug 'cas about: ' + key + ' begin'
      result = @client.cas(key,0,true,&proc)
      $log.debug 'cas about: ' + key + ' end with :' + result.to_s
    rescue =>e
      p e
      $log.fatal 'cas:client exception!!!' + e.to_s
      raise e
    end
    if result.nil?
      $log.debug('cas:' + key + ' not exist')
    elsif result[0..5] == 'STORED'
      $log.debug('cas:'+ key + ' success')
    elsif result[0..5] == 'EXISTS'
      $log.debug('cas:' + key + ' failed')
    else
      $log.fatal('cas:' + key + 'unexpected return : ' + p.to_s)
    end
    if result.nil? 
      p 'cas:not found in cas!!' + key.to_s
    end
    result
  end
  def get(key)
    @lock.lock
    begin
      result = @client.get(key,true)
      return nil if result.nil?
      $log.debug('get withl ' + key + ' -> ' + (result=="" ? "<empty>" : result.to_s))
    ensure
      @lock.unlock
    end
    result
  end
  def set(key,value)
    @lock.lock
    begin
      throw 'setting value is nil!!' if value.nil?
      p "key:" + key.to_s + " value:"+ value.to_s if key.nil? || value.nil?
      $log.debug('set with ' + key + ' -> ' + value)
      @client.set(key,value,0,true)
    ensure
      @lock.unlock
    end
  end
  def delete(key)
    @lock.lock
    begin
      $log.debug('delete ' + key.to_s)
      @client.delete(key)
    ensure
      @lock.unlock
    end
  end
  def flush_all
    @client.flush_all
  end
end

$devmemcached  = MemcacheWrap.new('localhost:11211')

class MemTransaction
  @@commit = 'commit'
  @@abort = 'abort'
  @@active = 'active'
  class Accessor
    @@commit = 'commit'
    @@abort = 'abort'
    @@active = 'active'
    
    def initialize(client, name)
      @client = client
      @name = name
    end
    def get(key)
      contention = ContentionManager.new(@client)
      $log.debug('transactional get begin')
      answer = nil
      loop{
        check_status
        begin
          data_to_delete = nil
          result = @client.cas(key){ |locator|
            break if locator == ""
            old,new,owner = MessagePack.unpack(locator)
            begin
              old_data = @client.get(old) unless old.nil?
              new_data = @client.get(new) unless new.nil?
              $log.info('get:' + key + ' -> ['+ old_data.to_s + '] [' + new_data.to_s + ']')
            rescue => e
              p e
              p e.backtrace
            end

            $log.debug('getter owner:' + owner + ' =?= ' + @name)
            if owner == @name
              answer = @client.get(new)
              raise AbortException  if answer.nil?
              $log.debug('transactional get: already own' + key + " -> "+answer)
              raise AlreadyOwn
            end
            owner_status = @client.get(owner)
            $log.debug('get:begin to rob the owning. Im ' + @name)
            next_old = nil
            if owner_status == @@commit
              $log.debug('transactional get: owner already commited, rob it by ' + @name + ' from ' + owner)
              next_old = new
              data_to_delete = old
            elsif owner_status == @@abort
              $log.debug('transactional get: owner already aborted, rob it by ' + @name+ ' from ' + owner)
              next_old = old
              data_to_delete = new
            elsif owner_status == @@active
              $log.debug 'get: retry! because ' + owner.to_s + 'is active!'
              contention.resolve(owner)
              raise RetryException
            else
              p 'why state is ' + owner + ' => '+ owner_status.to_s
              exit
            end

            answer = @client.get(next_old)
            raise AbortException if answer.nil?
            next_new = add_somewhere(answer) # clone
            $log.debug('get:try cas into [' + @client.get(next_old).to_s + ',' + @client.get(next_new) +","+ @name + ']')
            [next_old, next_new, @name].to_msgpack
          }
          if result == nil
            result= nil
            break
          elsif result[0..5] == 'EXISTS'
            $log.debug('get:cas retry')
            next
          end
          @client.delete(data_to_delete) unless data_to_delete.nil?
        rescue AlreadyOwn
          break
        rescue RetryException
          next
        rescue => e
          p e.backtrace
          $log.fatal('get:unexpected error ' + e.to_s)
          raise e
        end
        break
      }
      answer
    end
    def set(key,value)
      $log.debug('transactional set begin')
      contention = ContentionManager.new(@client)
      next_new = add_somewhere(value)
      loop{
        check_status
        begin
          data_to_delete = nil
          result = @client.cas(key){ |locator|
            $log.debug('locator:' + locator)
            old,new,owner = MessagePack.unpack(locator)
            begin
              old_data = @client.get(old) unless old.nil?
              new_data = @client.get(new) unless new.nil?
              $log.info('set:' + key + ' -> ['+ old_data.to_s + '] [' + new_data.to_s + ']')
            rescue => e
              p e
              p e.backtrace
            end
            
            $log.debug('owner:' + owner + ' =?= ' + @name)
            if owner == @name
              $log.debug('set:I already owned...')
              @client.set(new, value)
              raise AlreadyOwn
            else
              next_old = nil
              owner_status = @client.get(owner)
              $log.debug("status:" + owner_status)
              if owner_status == @@commit
                $log.debug 'set: owner seems to be commited, rob by ' + @name + 'from ' + owner
                data_to_delete = old
                next_old = new
                $log.debug('set:try cas into [' + next_old.to_s + ',' + next_new +","+ @name + ']')
              elsif owner_status == @@abort
                $log.debug 'set: owner seems to be aborted, rob by ' + @name + 'from ' + owner
                data_to_delete = new
                next_old = old
              elsif owner_status == @@active
                $log.debug 'set: retry! because ' + owner.to_s + 'is active!'
                contention.resolve(owner)
                raise RetryException
              end
              $log.debug('set:try cas into [' + @client.get(next_old).to_s + ',' + value.to_s + ","+ @name + ']')
              [new, next_new, @name].to_msgpack
            end
          }
        rescue RetryException => e
          $log.debug("retry transaction !")
          next
        rescue AlreadyOwn
          $log.debug 'set:overwrite ' + key + ' => ' + value.to_s
          break
        rescue AbortException => e
          @client.delete(next_new)
          raise e
        rescue => e
          $log.fatal('set:unexpected error ' + e.to_s)
          raise e
        end
        if result.nil?
          $log.debug('new saving:' + key + '->' + value)
          @client.set(key, [nil,add_somewhere(value),@name].to_msgpack)
          break
        elsif result[0..5] == 'EXISTS'
          $log.debug('set:cas retry')
          next
        elsif result[0..5] == 'STORED'
          @client.delete(data_to_delete) unless data_to_delete.nil?
          $log.debug('set:cas ok for ' + key)
          break
        else
          $log.fatal('invalid message!')
          exit
        end
      }
    end

    #privates
    def check_status
      status = @client.get(@name)
      raise AbortException.new if status == @@abort
      raise NotInTransaction.new if status == @@commit
    end
    private :check_status
    def add_somewhere(value)
      valuename = @name + ':' + rand(9999999999).to_s
      #      p valuename + ' -> ' + value
      $log.debug 'tmp name is ' + valuename.to_s + '=>' + value.to_s
      @client.set(valuename, value)
      return valuename
    end
    private :add_somewhere
    class ContentionManager
      @@commit = 'commit'
      @@abort = 'abort'
      @@active = 'active'
      def initialize(client)
        @counter = 0
        @client = client
      end
      def resolve(other_owner)
        sleep 0.001 * rand(1 << @counter)
        if @counter <= 10
          @counter += 1
        else
          $log.debug('contention: @' + @counter.to_s)
          rob_result = @client.cas(other_owner){ |value|
            break unless value == @@active
            $log.info('contention: do robbing cas!' + other_owner.to_s)
            @@abort
          }
          if rob_result.nil?
            $log.debug "contention:already resolved:" + other_owner
          elsif rob_result[0..5] == 'EXISTS'
            $log.debug 'contention:rob failed, retry'
          elsif rob_result[0..5] == 'STORED'
            $log.debug 'contention:rob success!!' + other_owner
          else
            $log.fatal 'contention: invelid cas result!'
          end
          @counter = 0
          $log.debug('contention:try rob and retry')
        end
      end
    end
  end
  def initialize(host)
    srand
    @client = MemcacheWrap.new host
  end
  $abortcounter = 0
  $successcounter = 0
  def transaction
    @t_name = 'transact1on' + (rand(100000000).to_s)
    begin
      loop {
        transact_result = 10
        begin
          @client.set(@t_name, @@active)
          transact_result = @client.cas(@t_name){ |value|
            raise AbortException unless value == @@active
            yield Accessor.new(@client, @t_name)
            @@commit
          }
          if transact_result.nil?
            p 'transaction:result nil!! ' + @t_name
          end
        rescue AbortException => e
          $abortcounter += 1
          $log.debug 'aborted! ' + @t_name + ' in ' + e.backtrace[0].to_s
          retry
        end
        
        if transact_result.nil?
          res = @client.get(@t_name)
          p 'not found for ' + @t_name + ':' + res.to_s
          res = @client.cas(@t_name){ 'active'}
          p 'not found and retry : ' + res.to_s
          exit
        end
        if transact_result[0..5] == 'STORED'
          $successcounter += 1
          $log.debug "abort counter:" + $abortcounter.to_s
          $log.debug "success counter:" + $successcounter.to_s
          $log.info "transaction:success !"
          break
        end
        $abortcounter += 1
        $log.info('transaction:retry !')
      }
    rescue => e
      p e.backtrace[0]
      $log.fatal('unexpected error in transaction' + e.to_s)
      raise e
    end
    $log.debug('transaction commit!!' + @t_name)
  end
end



