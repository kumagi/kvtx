require 'memcache'
require 'msgpack'
srand

class AbortExeption < Exception
end
class ActiveRetry < Exception
end
class NotInTransaction < Exception
end
class OkException < Exception
end
class NotFoundException < Exception
end

class MemcacheWrap
  def initialize(hostname)
    @client = MemCache.new(hostname)
  end
  def cas(key,&proc)
    @client.cas(key,0,true,&proc)
  end
  def get(key)
    @client.get(key,true)
  end
  def set(key,value)
    @client.set(key,value,0,true)
  end
  def delete(key)
    @client.delete(key)
  end
end


class MemTransaction
  class Accessor
    def initialize(client, name)
      @client = client
      @name = name
    end
    def get(key)
      retry_counter = 0
      loop{
        check_status

        result = @client.cas(key){ |locator|
          p key + 'locator#'+locator
          old,new,owner = MessagePack.unpack(locator)
          if owner == @name
            return @client.get(new)
          end
          begin
            owner_status = @client.get(owner)
          rescue => e
            p e
          end

          if owner_status == 'commited'
            return [old, add_somewhere(@client.get(new)), @name].to_msgpack
          elsif owner_status == 'abort'
            return [old, add_somewhere(@client.get(old)), @name].to_msgpack
          elsif owner_status == 'active'
            retry_counter += 1
            sleep 0.1
            p 'retry'
            next
          end
        }
        if result == nil
          return nil
        end
      }
    end
    def set(key,value)
      retry_counter = 0
      loop{
        check_status
        begin
          result = @client.cas(key){ |locator|
            p 'locator:' + locator
            old,new,owner = MessagePack.unpack(locator)
            p 'owner:' + owner + ' =?= ' + @name 
            
            if owner == @name
              @client.set(new,value)
              raise OkException.new
            else
              owner_status = @client.get(owner)
              p "status:" + owner_status
              if owner_status == 'commited'
                [new, add_somewhere(value), @name].to_msgpack
              elsif owner_status == 'abort'
                [old, add_somewhere(value), @name].to_msgpack
              elsif owner_status == 'active'
                retry_counter += 1
                sleep 0.3
                if retry_counter > 10
                  @client.cas(owner){ |value|
                    break unless value == 'active'
                    'abort'
                  }
                end
                raise
              end
            end
          }
        rescue OkException => e
          p "ok!!!!"
          break
        rescue => e
          p e
          retry
        end
        if result == nil
          @client.set(key, [nil,add_somewhere(value),@name].to_msgpack)
          next
        end
        p "result:"+result
        break
      }
    end

    #privates
    def check_status
      status = @client.get(@name)
      raise AbortExeption.new if status == 'abort'
      raise NotInTransaction.new if status == 'commited'
    end
    private :check_status
    def add_somewhere(value)
      valuename = @name + rand(999).to_s
      @client.set(valuename, value)
      return valuename
    end
    private :add_somewhere
  end
  def initialize(host)
    srand
    @client = MemcacheWrap.new host
    @status = 'commited'
    @t_name = 'transact1on' + (rand(1000000).to_s)
  end
  def transaction
    @client.set(@t_name, 'active')
    loop {
      result = @client.cas(@t_name){ |value|
        break unless value == 'active'
        begin
          yield Accessor.new(@client, @t_name)
        rescue => e
          p e
          raise e
        end
        'commited'
      }
      next if result == nil
      if result[0..5] == 'STORED'
        return
      end
    }
  end
end



