# frozen_string_literal: false
require 'drab/drab'
require 'monitor'

module DRab

  # Gateway id conversion forms a gateway between different DRab protocols or
  # networks.
  #
  # The gateway needs to install this id conversion and create servers for
  # each of the protocols or networks it will be a gateway between.  It then
  # needs to create a server that attaches to each of these networks.  For
  # example:
  #
  #   require 'drab'
  #   require 'unix'
  #   require 'drab/gw'
  #
  #   DRab.install_id_conv DRab::GWIdConv.new
  #   gw = DRab::GW.new
  #   s1 = DRab::DRabServer.new 'drabunix:/path/to/gateway', gw
  #   s2 = DRab::DRabServer.new 'druby://example:10000', gw
  #
  #   s1.thread.join
  #   s2.thread.join
  #
  # Each client must register services with the gateway, for example:
  #
  #   DRab.start_service 'drabunix:', nil # an anonymous server
  #   gw = DRabObject.new nil, 'drabunix:/path/to/gateway'
  #   gw[:unix] = some_service
  #   DRab.thread.join

  class GWIdConv < DRabIdConv
    def to_obj(ref) # :nodoc:
      if Array === ref && ref[0] == :DRabObject
        return DRabObject.new_with(ref[1], ref[2])
      end
      super(ref)
    end
  end

  # The GW provides a synchronized store for participants in the gateway to
  # communicate.

  class GW
    include MonitorMixin

    # Creates a new GW

    def initialize
      super()
      @hash = {}
    end

    # Retrieves +key+ from the GW

    def [](key)
      synchronize do
        @hash[key]
      end
    end

    # Stores value +v+ at +key+ in the GW

    def []=(key, v)
      synchronize do
        @hash[key] = v
      end
    end
  end

  class DRabObject # :nodoc:
    def self._load(s)
      #uri, ref = Marshal::load(s)
      uri, ref = JSON::load(s)
      if DRab.uri == uri
        return ref ? DRab.to_obj(ref) : DRab.front
      end

      self.new_with(DRab.uri, [:DRabObject, uri, ref])
    end

    def _dump(lv)
      if DRab.uri == @uri
        if Array === @ref && @ref[0] == :DRabObject
          #Marshal::dump([@ref[1], @ref[2]])
          JSON::dump([@ref[1], @ref[2]])
        else
          #Marshal::dump([@uri, @ref]) # ??
          JSON::dump([@uri, @ref]) # ??
        end
      else
        #Marshal::dump([DRab.uri, [:DRabObject, @uri, @ref]])
        JSON::dump([DRab.uri, [:DRabObject, @uri, @ref]])
      end
    end
  end
end

=begin
DRab.install_id_conv(DRab::GWIdConv.new)

front = DRab::GW.new

s1 = DRab::DRabServer.new('drabunix:/tmp/gw_b_a', front)
s2 = DRab::DRabServer.new('drabunix:/tmp/gw_b_c', front)

s1.thread.join
s2.thread.join
=end

=begin
# foo.rb

require 'drab/drab'

class Foo
  include DRabUndumped
  def initialize(name, peer=nil)
    @name = name
    @peer = peer
  end

  def ping(obj)
    puts "#{@name}: ping: #{obj.inspect}"
    @peer.ping(self) if @peer
  end
end
=end

=begin
# gw_a.rb
require 'drab/unix'
require 'foo'

obj = Foo.new('a')
DRab.start_service("drabunix:/tmp/gw_a", obj)

robj = DRabObject.new_with_uri('drabunix:/tmp/gw_b_a')
robj[:a] = obj

DRab.thread.join
=end

=begin
# gw_c.rb
require 'drab/unix'
require 'foo'

foo = Foo.new('c', nil)

DRab.start_service("drabunix:/tmp/gw_c", nil)

robj = DRabObject.new_with_uri("drabunix:/tmp/gw_b_c")

puts "c->b"
a = robj[:a]
sleep 2

a.ping(foo)

DRab.thread.join
=end

