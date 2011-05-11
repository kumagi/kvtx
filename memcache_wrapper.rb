=begin
Memcache wrapper for Memcached Library
=end
require 'memcached'


class MemcacheWrap
  class CAS_CHANGED < Exception
  end
  class CAS_NOT_EXIST < Exception
  end
  def initialize(hostname)
    @client = Memcached.new([hostname], :support_cas => true, :tcp_nodelay => true)
    @counter = {'set'=>0, 'get'=> 0, 'cas'=>0, 'delete' => 0}
  end
  attr_reader :counter
  def cas(key,&proc)
    @counter['cas'] += 1
    begin
      result = @client.cas(key,&proc)
    rescue Memcached::ConnectionDataExists
      raise CAS_CHANGED
    rescue Memcached::NotFound
      raise CAS_NOT_EXIST
    end
    true
  end
  def get(key)
    @counter['get'] += 1
    begin
      result = @client.get(key)
    rescue Memcached::NotFound
      return nil
    end
    result
  end
  def set(key,value)
    @counter['set'] += 1
    throw 'setting value is nil!!' if value.nil?
    @client.set(key,value,0)
  end
  def delete(key)
    $log.debug('delete ' + key.to_s)
    begin
      @client.delete(key)
    rescue Memcached::NotFound
      return false
    end
    true
  end
  def flush_all
    @client.flush
  end
end
