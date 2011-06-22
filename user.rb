require './memtransaction'

raw = MemCache.new('localhost:11211')
#raw.delete('k1')

tr = MemTransaction.new('localhost:11211')

tr.transaction{|n|
  n.set('k1','v1')
  n.set('k1','v2')
  n.set('k2','v3')
}
p 'commit!' + raw.get('k1', true)
p 'commit!' + raw.get('k2', true)
t=nil
s=nil
tr.transaction{|n|
  p n.get('k1')
  p n.get('k3')
  p n.get('k2')
}

