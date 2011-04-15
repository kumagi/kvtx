# -*- coding: utf-8 -*-
require 'msgpack/rpc'

cli = MessagePack::RPC::Client.new("0.0.0.0", 5000)
cli.timeout = 5
# helloメソッドを呼び出す
packed = MessagePack.pack(["hello","hello",2])
puts cli.call(:hello, packed)

exit
