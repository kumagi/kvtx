
require './memtransaction'
#require 'memcached'

def bench_time
  before = Time.now
  yield
  Time.now - before
end

hostname = 'localhost:12121'

#=begin
@tr = MemTransaction.new(hostname)
# @tr.transaction{ |n|
#  n.set('cnt','0')
# }
num = 10000
puts (num / bench_time { 
        10000.times{
          @tr.transaction{ |n|
            n.set('hogege' + n.to_s, 'd')
          }
#          @tr.transaction{ |n|
#            n.set('cnt', (n.get('cnt').to_i + 1).to_s)
#          }
        }
      }).to_s + " transaction / sec"

#=end

=begin
memcached = Memcached.new(hostname)
print 'set speed: '
num = 100000
puts (num / bench_time{
  100000.times{|n|
    memcached.set('x'+ n.to_s, n.to_s)
  }
}).to_s + " sets / sec"
print 'get speed: '
puts (num / bench_time{
  100000.times{|n|
    memcached.get('x'+ n.to_s)
  }
}).to_s + " gets / sec"
=end

@tr.dump_status
