# frozen_string_literal: false
=begin
 external service
        Copyright (c) 2000,2002 Masatoshi SEKI
=end

require 'drab/drab'
require 'monitor'

module DRab
  class ExtServ
    include MonitorMixin
    include DRabUndumped

    def initialize(there, name, server=nil)
      super()
      @server = server || DRab::primary_server
      @name = name
      ro = DRabObject.new(nil, there)
      synchronize do
        @invoker = ro.regist(name, DRabObject.new(self, @server.uri))
      end
    end
    attr_reader :server

    def front
      DRabObject.new(nil, @server.uri)
    end

    def stop_service
      synchronize do
        @invoker.unregist(@name)
        server = @server
        @server = nil
        server.stop_service
        true
      end
    end

    def alive?
      @server ? @server.alive? : false
    end
  end
end
