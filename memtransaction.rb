require 'memcache'
require 'msgpack'
require 'logger'
require 'ruby-debug'
srand

class AbortExeption < Exception
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
#$log.level=Logger::INFO

$log.datetime_format=''


class MemcacheWrap
  def initialize(hostname)
    @client = MemCache.new(hostname)
    @lock = Mutex.new
  end
  def cas(key,&proc)
    $log.debug('cas ' + key)
    begin
      p 'cas about: ' + key + ' begin'
      result = @client.cas(key,0,true,&proc)
      p 'cas about: ' + key + ' end with :' + result.to_s
    rescue=>e
      p 'cas:client exception'
      p e
    end
    if result.nil?
      $log.debug('cas:' + key + ' not exist')
    elsif result[0..5] == 'STORED'
      $log.debug('cas:'+ key + ' success')
    elsif result[0..5] == 'EXISTS'
      $log.debug('cas:' + key + ' failed')
    else
      $log.fatal('cas:' + key + 'unexpected return : ' + p)
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
      $log.debug('get withl ' + key + ' -> ' + (result=="" ? "empty" : result.to_s))
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
      retry_counter = 0
      $log.debug('transactional get begin')
      answer = nil
      loop{
        check_status
        begin
          result = @client.cas(key){ |locator|
            break if locator == ""
            old,new,owner = MessagePack.unpack(locator)
            begin
              $log.debug(key + ' -> ' + '['+ (old.nil? ? "" : ($devmemcached.get(old))) + '] [' + (new.nil? ? "" : ($devmemcached.get(new))) + ']')
            rescue => e
            end

            $log.debug('getter owner:' + owner + ' =?= ' + @name)
            if owner == @name
              answer = @client.get(new)
              raise AbortExeption  if answer.nil?
              $log.debug('transactional get: already own' + key + " -> "+answer)
              raise AlreadyOwn
            end
            owner_status = @client.get(owner)
            $log.debug('begin to rob the owning')
            next_old = nil
            if owner_status == @@commit
              $log.debug('transactional get: owner already commited, rob it from new' + @name)
              next_old = new
              @client.delete(old) unless old.nil?
              answer = @client.get(new)
            elsif owner_status == @@abort
              $log.debug('transactional get: owner already aborter, rob it from old ' + @name)
              next_old = old
              @client.delete(new) unless new.nil?
              answer = @client.get(old)
            elsif owner_status == @@active
              retry_counter += 1
              sleep 0.01
              $log.debug('get:active waiting... ' + retry_counter.to_s)
              if retry_counter > 10
                  $log.debug('get:rob from active thread !!make abort ' + retry_counter.to_s)
                  p @client.cas(owner){ |value|
                    break unless value == @@active
                    @@abort
                  }
              end
              raise RetryException
            else
              p 'why state is '+ status.to_s
              exit
            end
            raise AbortExeption if answer.nil?
            p "old:" + old + " new:" + new if answer.nil?
            next_new = add_somewhere(answer)
            $log.debug('try cas into [' + next_old + ',' + next_new +","+ @name + ']')
            [next_old, next_new, @name].to_msgpack
          }
          if result == nil
            result= nil
            break
          elsif result[0..5] == 'EXISTS'
            $log.debug('get:cas retry')
            next
          end
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
      retry_counter = 0
      loop{
        check_status
        begin
          result = @client.cas(key){ |locator|
            $log.debug('locator:' + locator)
            begin
              old,new,owner = MessagePack.unpack(locator)
            rescue MessagePack::UnpackError
              $log.debug('set: unpack failed! retry.')
              locator = @client.get(key)
              retry
            end
            $log.debug(key + ' -> ' + '['+ (old.nil? ? "" : ($devmemcached.get(old).to_s)) + '] [' + (new.nil? ? "" : ($devmemcached.get(new).to_s)) + ']')
            $log.debug('owner:' + owner + ' =?= ' + @name)
            if owner == @name
              $log.debug('set:I already owned...')
              next_new = add_somewhere(value)
              [new, next_new, @name].to_msgpack
            else
              owner_status = @client.get(owner)
              $log.debug("status:" + owner_status)
              if owner_status == @@commit
                @client.delete(old) unless old.nil?
                [new, add_somewhere(value), @name].to_msgpack
              elsif owner_status == @@abort
                @client.delete(new)
                [old, add_somewhere(value), @name].to_msgpack
              elsif owner_status == @@active
                sleep 0.01
                retry_counter += 1
                $log.debug('set:retry!! @ ' + retry_counter.to_s)
                if retry_counter > 10
                  @client.cas(owner){ |value|
                    break unless value == @@active
                    @@abort
                  }
                  $log.debug('aborting')
                end
                raise ActiveRetry
              end
            end
          }
        rescue ActiveRetry => e
          $log.debug("retry transaction !")
          next
        rescue RetryException
          next
        rescue => e
          $log.fatal('set:unexpected error ' + e.to_s)
          raise e
        end
        if result.nil?
          $log.debug('new saving:' + key + '->' + value)
          @client.set(key, [nil,add_somewhere(value),@name].to_msgpack)
          return nil
        elsif result[0..5] == 'EXISTS'
          $log.debug('set:cas retry')
          next
        elsif result[0..5] == 'STORED'
          $log.debug('set:cas ok!!')
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
      raise AbortExeption.new if status == @@abort
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
  end
  def initialize(host)
    srand
    @client = MemcacheWrap.new host
    @t_name = 'transact1on' + (rand(1000000).to_s)
  end
  $abortcounter = 0
  $successcounter = 0
  def transaction
    begin
      loop {
        transact_result = 10
        @client.set(@t_name, @@active)
        begin
          transact_result = @client.cas(@t_name){ |value|
            raise AbortException unless value == @@active
            yield Accessor.new(@client, @t_name)
            @@commit
          }
          if transact_result.nil?
            p 'transaction:result nil!! ' + @t_name
          end
        rescue AbortExeption
          $abortcounter += 1
          p 'aborted'
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
          break
        end
        $abortcounter += 1
        $log.debug('transactional retry!')
      }
    rescue => e
      p e.backtrace[0]
      $log.fatal('unexpected error in transaction' + e.to_s)
    end
  end
end



