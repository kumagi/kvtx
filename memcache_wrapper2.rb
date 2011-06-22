=begin
Memcache wrapper for Memcache-Client Library
=end   

require 'memcache'
class MemcacheWrap
  class CAS_CHANGED < Exception
  end
  class CAS_NOT_EXIST < Exception
  end
  def initialize(hostname)
    @client = MemCache.new(hostname)
    @counter = {'set'=>0, 'get'=> 0, 'cas'=>0, 'delete' => 0}
  end
  attr_reader :counter
  def cas(key,&proc)
    @counter['cas'] += 1
    begin
      result = @client.cas(key,0,true,&proc)
    rescue =>e
      raise e
    end
    if result.nil?
      raise CAS_NOT_EXIST
    elsif result[0..5] == 'EXISTS'
      raise CAS_CHANGED
    end
    true
  end
  def get(key)
    @counter['get'] += 1
    result = @client.get(key,true)
    return nil if result.nil?
    result
  end
  def set(key,value)
    @counter['set'] += 1
    throw 'setting value is nil!!' if value.nil?
    @client.set(key,value,0,true)
  end
  def add(key,value)
    @counter['set'] += 1
    throw 'setting value is nil!!' if value.nil?
    @client.set(key,value,0,true)
  end
  def delete(key)
    @client.delete(key)
  end
  def flush_all
    @client.flush_all
  end
end
