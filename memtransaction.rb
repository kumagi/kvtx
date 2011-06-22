require 'msgpack'
require 'logger'
require 'ap'
require './memcache_wrapper'
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
# $log.level=Logger::INFO
$log.level=Logger::FATAL

$log.datetime_format=''

def random_string(len)
  random_pattern = ('a'..'z').to_a + ('A'..'Z').to_a + ('0'..'9').to_a # + '!@#$%^&*()_+{}:;-=][/.,<>?"'
  ans = (Array.new(len){
           random_pattern[rand(random_pattern.size)]
         }
         ).join
end

module MemTransactionConst
  COMMIT = "1"
  ABORT = "2"
  ACTIVE = "3"
  ACTIVE_NONCOPY = "4"
end

$devmemcached  = MemcacheWrap.new('localhost:11211')


module RandomAdd
  def random_add(value)
    loop{
      key = random_string(31)
      result = @client.add(key, value)
      return key if result == true
    }
    end
end

class MemTransaction
  include MemTransactionConst
  include RandomAdd
  class Accessor
    include MemTransactionConst
    include RandomAdd
    def initialize(client, name)
      @client = client
      @name = name
    end
    private :random_add
    def get(key)
      contention = ContentionManager.new(@client)
      $log.debug('transactional get begin')
      answer = nil
      loop{
        #check_status
        begin
          data_to_delete = nil
          result = @client.cas(key){ |locator|
            old,new,owner = MessagePack.unpack(locator)

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
            if owner_status == COMMIT
              $log.debug('transactional get: owner already commited, rob it by ' + @name + ' from ' + owner)
              next_old = new
              data_to_delete = old
            elsif owner_status == ABORT
              $log.debug('transactional get: owner already aborted, rob it by ' + @name+ ' from ' + owner)
              next_old = old
              data_to_delete = new
            elsif owner_status == ACTIVE
              $log.debug 'get: retry! because ' + owner.to_s + 'is active!'
              contention.resolve(owner)
              raise RetryException
            else
              p 'why state is ' + owner + ' => '+ owner_status.to_s
              exit
            end
            answer = @client.get(next_old)
            raise AbortException if answer.nil?
            next_new = random_add(answer) # clone
            $log.debug('get:try cas into [' + answer.to_s + ',' + answer.to_s + "," + @name + ']')
            [next_old, next_new, @name].to_msgpack
          }
        rescue MemcacheWrap::CAS_NOT_EXIST
          @client.set(key, [nil, nil, @name].to_msgpack)
          answer = nil
          break
        rescue MemcacheWrap::CAS_CHANGED
          $log.debug('get:cas retry')
          next
        rescue AlreadyOwn
          break
        rescue RetryException
          next
        rescue AbortException => e
          raise e
        rescue => e
          p e.backtrace
          $log.fatal('get:unexpected error ' + e.to_s)
          raise e
        end
        # success!
        @client.delete(data_to_delete) unless data_to_delete.nil?
        break
      }
      answer
    end
    def set(key,value)
      $log.debug('transactional set begin')
      contention = ContentionManager.new(@client)
      next_new = random_add(value)
      loop{
        #check_status
        begin
          data_to_delete = nil
          result = @client.cas(key){ |locator|
            old,new,owner = MessagePack.unpack(locator)
            $log.debug('owner:' + owner + ' =?= ' + @name)
            if owner == @name
              @client.set(new, value)
              raise AlreadyOwn
            else
              next_old = nil
              owner_status = @client.get(owner)
              $log.debug("status:" + owner_status)
              if owner_status == COMMIT
                $log.debug 'set: owner seems to be commited, rob by ' + @name + 'from ' + owner
                data_to_delete = old
                next_old = new
                $log.debug('set:try cas into [' + next_old.to_s + ',' + next_new +","+ @name + ']')
              elsif owner_status == ABORT
                $log.debug 'set: owner seems to be aborted, rob by ' + @name + 'from ' + owner
                data_to_delete = new
                next_old = old
              elsif owner_status == ACTIVE
                $log.debug 'set: retry! because ' + owner.to_s + 'is active!'
                contention.resolve(owner)
                raise RetryException
              else
                $log.fatal 'set:unknown status ' + owner_status
              end
              [new, next_new, @name].to_msgpack
            end
          }
        rescue MemcacheWrap::CAS_NOT_EXIST
          @client.set(key,[nil, next_new, @name].to_msgpack)
          break
        rescue MemcacheWrap::CAS_CHANGED
          $log.debug('set:cas retry')          
          next
        rescue RetryException
          $log.debug("retry transaction !")
          next
        rescue AlreadyOwn
          $log.debug 'set:overwrite ' + key + ' => ' + value.to_s
          break
        rescue AbortException
          @client.delete(next_new)
          raise e
        rescue => e
          $log.fatal('set:unexpected error ' + e.to_s)
          raise e
        end
        @client.delete(data_to_delete) unless data_to_delete.nil?
        $log.debug('set:cas ok for ' + key)
        break
      }
    end

    #privates
    def check_status
      status = @client.get(@name)
      raise AbortException.new if status == ABORT
      raise NotInTransaction.new if status == COMMIT
    end
    private :check_status
    class ContentionManager
      include MemTransactionConst
      def initialize(client)
        @counter = 0
        @client = client
      end
      def resolve(other_owner)
        sleep 0.0001 * rand(1 << @counter)
        if @counter <= 10
          @counter += 1
        else
          $log.debug('contention: @' + @counter.to_s)
          begin
            @client.cas(other_owner){ |value|
              raise MemcacheWrap::CAS_CHANGED unless value == ACTIVE
              $log.info('contention: do robbing cas!' + other_owner.to_s)
              ABORT
            }
          rescue MemcacheWrap::CAS_NOT_EXIST
            $log.fatal 'what happen?'
          rescue MemcacheWrap::CAS_CHANGED
            $log.debug "contention:already resolved:" + other_owner
          end
          @counter = 0
          $log.debug('contention:tried rob and retry')
        end
      end
    end
  end
  def initialize(host, name = 'tran:')
    srand
    @client = MemcacheWrap.new host
    @transaction_name = name
  end
  $abortcounter = 0
  $successcounter = 0
  
  def transaction
    begin
      loop {
        transact_result = 10
        begin
          @t_name = random_add(ACTIVE)
          @client.cas(@t_name){ |value|
            raise AbortException unless value == ACTIVE
            yield Accessor.new(@client, @t_name)
            COMMIT
          }
        rescue MemcacheWrap::CAS_CHANGED
          $abortcounter += 1
          $log.info 'transaction:retry!'
          retry
        rescue AbortException => e
          $abortcounter += 1
          $log.debug 'aborted! ' + @t_name + ' in ' + e.backtrace[0].to_s
          retry
        end
        
        $successcounter += 1
        $log.debug "abort counter:" + $abortcounter.to_s
        $log.debug "success counter:" + $successcounter.to_s
        $log.info "transaction:success !"
        break
      }
    rescue => e
      p e.backtrace
      $log.fatal('unexpected error in transaction ' + e.to_s)
      raise e
    end
    $log.debug('transaction commit!! ' + @t_name)
  end
  def dump_status
    print "commited:" + $successcounter.to_s + "\n"
    print "aborted:" + $abortcounter.to_s + "\n"
    ap @client.counter
  end
  def total_calls
    @client.counter['set'] + @client.counter['get'] + @client.counter['cas'] + @client.counter['delete']
  end
end



