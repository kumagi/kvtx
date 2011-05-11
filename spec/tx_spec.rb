require './memtransaction'


class MockClient
  def initialize(host)
    @client = Object.new
  end
end

def get_new(key)
  memcached = MemcacheWrap.new('localhost:11211')
  memcached.get(MessagePack.unpack(memcached.get(key))[1])
end
def get_old(key)
  memcached = MemcacheWrap.new('localhost:11211')
  memcached.get(MessagePack.unpack(memcached.get(key))[0])
end

describe MemTransaction do
  before do
    #pending
    @memcached = MemcacheWrap.new('localhost:11211')
    @memcached.flush_all
    @tr = MemTransaction.new('localhost:11211')
    @name = @tr.instance_eval{ @t_name }
  end
  it "success one commit" do
    @tr.transaction{|n|
      n.set('a','b')
    }
    @memcached.get('a').should_not == nil
    get_new('a').should == 'b'
  end
  it "success two commit" do
    @tr.transaction{|n|
      n.set('a','b')
      n.set('c','d')
    }
    get_new('a').should == 'b'
    get_new('c').should == 'd'
  end
  it "success two transaction" do
    @tr.transaction{|n|
      n.set('a','b')
    }
    get_new('a').should == 'b'
    @tr.transaction{|n|
      n.set('a','c')
    }
    get_new('a').should == 'c'
    #get_old('a').should == 'b'
  end
  it "success many transaction" do
    #pending "noisy"
    100.times {|n|
      @tr.transaction{|o|
        key = 'a' + n.to_s
        value = 'b' + n.to_s
        o.set(key,value)
      }
    }
    100.times { |n|
      get_new('a' + n.to_s).should == 'b' + n.to_s
    }
  end
  it "fail one transaction" do
    #pending "it's heavy"
    @tr.transaction{|n|
      n.set('a','b')
    }
    retrying = 10
    trynum = 0
    retrynum = 0
    @tr.transaction{|n| # will fail
      retrynum += 1
      n.set('a','c')
      n.set('b','d')
      if trynum < retrying
        MemTransaction.new('localhost:11211').transaction{ |m|
          #p "over writing b"
          m.set('b','e')
        }
        trynum += 1
      end
    }
    #p 'new of a:' + get_new('a')
    #p 'new of b:' + get_new('b')
    get_new('b').should == 'd'
    get_new('a').should == 'c'
    retrynum.should == retrying+1
  end
  it 'continuous repeat' do
    #pending "its heavy"
    init = MemTransaction.new('localhost')
    init.transaction{ |n|
      n.set('cnt', '4')
    }
    12.times { |n|
      12.times{
        #        p 'before:' + get_new('cnt')
        procedure = MemTransaction.new('localhost:11211')
        #        p 'init:' + get_new('cnt')
        procedure.transaction{ |m|
          #          p 'new:' + get_new('cnt')
          #          p 'old:' + get_old('cnt')
          t = m.get('cnt').to_i
          #          p 'got:' + t.to_s
          newvalue = (t+1).to_s
          m.set('cnt', newvalue)
          #          p t.to_s + ' -> ' + (t.to_i+1).to_s
        }
        #        p get_new('cnt')
      }
    }
    get_new('cnt').should == '148'
  end
end

describe MemTransaction do
  before do
    initialize = MemTransaction.new('localhost:11211')
    @memcached = MemcacheWrap.new('localhost:11211')    
    @memcached.flush_all
    initialize.transaction{ |m|
      m.set('counter', '0')
    }
  end
  it 'transaction saves consistency' do
    #pending 
    threads = []
    count = 0
    100.times { |n|
      threads << Thread.new do
        begin
          procedure = MemTransaction.new('localhost:11211') 
          10.times{
            procedure.transaction{ |m|
              t = m.get('counter').to_i
              newvalue = (t+1).to_s
              m.set('counter', newvalue)
            }
            count +=1
          }
        rescue => e
          p 'FATAL!!thread exception!'
          p e
        end
      end
    }
    threads.each { |n|
      n.join
    }
    p 'count :' + count.to_s
    get_new('counter').should == '1000'
  end
end
