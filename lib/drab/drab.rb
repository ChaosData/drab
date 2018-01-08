# frozen_string_literal: false
#
# = ./drab.rb
#
# Restricted Distributed Ruby: drab
#
# Copyright (c) 1999-2003 Masatoshi SEKI.  You can redistribute it and/or
# modify it under the same terms as Ruby.
#
# Author:: Masatoshi SEKI, Jeff Dileo
#

require 'socket'
require 'thread'
require 'io/wait'
require 'drab/eq'
require 'json'
require 'marshal/structure'
require 'set'

module DRab

  class DRabError < RuntimeError; end
  class DRabConnError < DRabError; end

  class DRabIdConv
    @@ids = {} # totally a memory leak, but not much that can be done
                      # unless there's a way to remove recycled objects' ids
    

    def to_obj(ref)
      if @@ids.include?(ref)
        #ObjectSpace._id2ref(ref)
        @@ids[ref]
      else
        STDERR.puts "attempted to load an unshared object by object id"
        raise Exception.new("attempted to load an unshared object by object id")
      end
    end

    def to_id(obj)
      ret = obj.nil? ? nil : obj.__id__
      #@@id_list.add(ret)
      @@ids[ret] = obj
      ret
    end
  end

  module DRabUndumped
    def _dump(dummy)
      raise TypeError, "can't/won't dump"
    end
  end

  class DRabServerNotFound < DRabError; end
  class DRabBadURI < DRabError; end
  class DRabBadScheme < DRabError; end

  class DRabUnknownError < DRabError

    def initialize(unknown)
      @unknown = unknown
      super(unknown.name)
    end

    attr_reader :unknown

    #def self._load(s)  # :nodoc:
    #  Marshal::load(s)
    #end

    #def _dump(lv) # :nodoc:
    #  Marshal::dump(@unknown)
    #end
  end

  class DRabRemoteError < DRabError

    def initialize(error)
      @reason = error.class.to_s
      super("#{error.message} (#{error.class})")
      set_backtrace(error.backtrace)
    end

    attr_reader :reason
  end

  class DRabUnknown

    def initialize(err, buf)
      case err.to_s
      when /uninitialized constant (\S+)/
        @name = $1
      when /undefined class\/module (\S+)/
        @name = $1
      else
        @name = nil
      end
      @buf = buf
    end

    attr_reader :name

    attr_reader :buf

    #def self._load(s) # :nodoc:
    #  begin
    #    Marshal::load(s)
    #  rescue NameError, ArgumentError
    #    DRabUnknown.new($!, s)
    #  end
    #end

    #def _dump(lv) # :nodoc:
    #  @buf
    #end

    #def reload
    #  self.class._load(@buf)
    #end

    def exception
      DRabUnknownError.new(self)
    end
  end

  class DRabArray
    class Token
      def initialize(sz)
        @size = sz
      end

      def size
        @size
      end

      def self._load(s)
        sz = JSON::load(s)
        if sz.is_a?(Integer)
          self.new(sz)
        else
          raise Exception.new("got non-integer JSON when parsing DRabArray")
        end
      end

      def _dump(lv)
        JSON::dump(@size)
      end
    end

    def initialize(ary)
      @ary = ary.collect { |obj|
        if obj.kind_of? DRabUndumped
          DRabObject.new(obj)
        else
          begin
            obj
          rescue
            DRabObject.new(obj)
          end
        end
      }
    end

    def get
      @ary
    end

    def self._load(s)
      raise Exception.new("not marshallable")
    end

    def self._dump(lv)
      raise Exception.new("not marshallable")
    end
  end 


  class DRabMessage
    def initialize(config)
      @load_limit = config[:load_limit]
      @argc_limit = config[:argc_limit]
    end

    def dump(obj, error=false, as_json = false)
      if as_json
        str = JSON::dump(obj)
        return [str.size].pack('N') + str
      end

      if obj.is_a?(DRabUndumped)
        obj = make_proxy(obj, error)
      elsif obj.is_a?(Proc)
        obj = make_proxy(obj, error)        
      elsif obj.is_a?(DRabArray)
        p = dump(DRabArray::Token.new(obj.get.size), error)
        obj.get.each { |o| p += dump(o) }
        return p
      end

      begin
        str = Marshal::dump(obj)
      rescue
        #str = Marshal::dump(make_proxy(obj, error))
        STDERR.puts "failed to marshal value of type " + obj.class.to_s + ", not exposing a proxy"
        str = Marshal::dump(nil)
      end
      [str.size].pack('N') + str
    end

    def load(soc, as_json = false)
      begin
        sz = soc.read(4)
      rescue
        raise(DRabConnError, $!.message, $!.backtrace)
      end
      raise(DRabConnError, 'connection closed') if sz.nil?
      raise(DRabConnError, 'premature header') if sz.size < 4
      sz = sz.unpack('N')[0]
      raise(DRabConnError, "too large packet #{sz}") if @load_limit < sz
      begin
        str = soc.read(sz)
      rescue
        raise(DRabConnError, $!.message, $!.backtrace)
      end
      raise(DRabConnError, 'connection closed') if str.nil?
      raise(DRabConnError, 'premature marshal format(can\'t read)') if str.size < sz
      
      if not as_json
        begin
          typ, structure = parse_marshal(str)
        rescue Exception => e
        end
      end

      DRab.mutex.synchronize do
        begin
          save = Thread.current[:drab_untaint]
          Thread.current[:drab_untaint] = []
          if as_json
            JSON::load(str)
          else
            if typ.between?(Types::Bln, Types::Arr)
              Marshal::load(str)
            elsif typ === Types::Err
              Exception.new(s.to_s)
            else
              STDERR.puts "got weird object: " + str.inspect + "\nwith structure: " + structure.inspect
              nil
            end
          end
        rescue NameError, ArgumentError => e
          puts e
          puts e.backtrace
          DRabUnknown.new($!, str)
        ensure
          Thread.current[:drab_untaint].each do |x|
            x.untaint
          end
          Thread.current[:drab_untaint] = save
        end
      end
    end

    def send_request(stream, ref, msg_id, arg, b)
      ary = []
      ary.push(dump(ref.__drabref, false, true))
      ary.push(dump(msg_id.id2name, false, true))
      ary.push(dump(arg.length, false, true))
      arg.each do |e|
        ary.push(dump(e))
      end
      #ary.push(dump(b))

      send_secret(stream)
      stream.write(ary.join(''))
    rescue
      raise(DRabConnError, $!.message, $!.backtrace)
    end

    def match_structures(template, target)
      begin
        if not (template.is_a?(Array) and target.is_a?(Array))
          return false
        end

        if template.size != target.size
          return false
        end

        return (0...template.size).collect do |n|
          if template[n].is_a?(Array)
            hld = match_structures(template[n], target[n])
            if !hld
              return [false]
            else
              next hld
            end
          end

          if template[n] === target[n]
            next true
          end

          if template[n].is_a?(String) and target[n].is_a?(String)
            if template[n] === "*"
              next true
            elsif template[n].start_with?("*")
              next target[n].end_with?(template[n][1..-1])
            elsif template[n].end_with?("*")
              next target[n].start_with?(template[n][0..-2])
            else
              return [false]
            end
          end
        end.inject { |t, n| t and n }
      rescue Exception => e
        STDERR.puts e
        STDERR.puts e.backtrace
      end     
      return false
    end

    module Types
      Ukn = 0
      Bln = 1
      Nil = 2
      Sym = 3
      Num = 4
      DRb = 5
      Str = 6
      Arr = 7
      Err = 8
    end


    def parse_marshal(str)
      begin
        # i don't know why, but if this runs in the current thread,
        # the __recursive_key__ state in PP, as invoked by pry, gets mangled
        msthread = Thread.new { Thread.current[:structure] = Marshal::Structure::load(str) }
        msthread.join
        parsed_str = msthread[:structure]
        if parsed_str === :true or parsed_str === :false
          return Types::Bln
        elsif parsed_str === :nil
          return Types::Nil
        elsif parsed_str === [:symbol, 0, "write"] # :write
          return Types::Sym
        elsif parsed_str === [:symbol, 0, "readline"] # :readline
          return Types::Sym
        elsif parsed_str.size === 2 and parsed_str[0] === :fixnum and parsed_str[1].is_a?(Integer)
          return Types::Num
        elsif match_structures([ # json DRab::DRabObject
          :user_defined,
          0,
          [:symbol, 0, "DRab::DRabObject"],
          "*"
        ], parsed_str)
          return [Types::DRb, parsed_str]
        elsif match_structures([ # json DRab::DRabObject
          :instance_variables,
          [
            :user_defined,
            0,
            [:symbol, 0, "DRab::DRabObject"],
            "*"
          ],
          1,
          [:symbol, 1, "E"],
          :true
        ], parsed_str)
          return [Types::DRb, parsed_str]
        elsif match_structures([ # string
          :string,
          0,
          "*"
        ], parsed_str)
          return Types::Str
        elsif match_structures([ # string
          :instance_variables,
          [:string, 0, "*"],
          1,
          [:symbol, 0, "E"],
          :false
        ], parsed_str)
          return Types::Str
        elsif match_structures([ # json DRab::DRabArray::Token
          :instance_variables,
          [
            :user_defined,
            0,
            [:symbol, 0, "DRab::DRabArray::Token"],
            "*"
          ],
          1,
          [:symbol, 1, "E"],
          :true
        ], parsed_str)
          return [Types::Arr, parsed_str]
        elsif match_structures([
          [:object, 0, [:symbol, 0, "*Error"]]
        ], parsed_str[0..2])
          return [Types::Err, parsed_str]
        else
          return [Types::Ukn, parsed_str]
        end
      rescue Exception => e
        STDERR.puts e
        STDERR.puts e.backtrace
      end
      return [Types::Ukn, parsed_str]
    end

    BLACKLISTED_BASIC_OBJECT_METHODS = Set.new([
        "send",
        "__send__",
        "method",
        "methods",
        "inspect",
        "private_methods",
        "protected_methods",
        "public_method",
        "public_methods",
        "public_send",
        "singleton_class",
        "singleton_method",
        "singleton_methods",
        "taint",
        "trust",
        "untaint",
        "untrust",
        "instance_eval",
        "instance_exec",
        "method_missing",
        "singleton_method_added",
        "singleton_method_removed",
        "instance_variables",
        "instance_variable_get",
        "instance_variable_set",
        "instance_variable_defined?",
        "remove_instance_variable",
    ])

    BLACKLISTED_MODULE_METHODS = Set.new([
        "used_modules",
        "alias_method",
        "append_features",
        "attr",
        "attr_reader",
        "attr_accessor",
        "attr_writer",
        "define_method",
        "extend_object",
        "extended",
        "include",
        "included",
        "included_modules",
        "instance_method",
        "instance_methods",
        "module_eval",
        "module_exec",
        "prepend",
        "private_class_method",
        "private_constant",
        "private_instance_methods",
        "protected_instance_methods",
        "public_class_method",
        "public_instance_method",
        "public_instance_methods",
        "remove_class_variable",
        "constants",
        "nesting",
        "ancestors",
        "autoload",
        "class_eval",
        "class_exec",
        "class_variable_get",
        "class_variable_set",
        "class_variables",
        "const_get",
        "const_set",
        "const_missing",
        "deprecate_constants",
        "method_added",
        "method_removed",
        "method_undefined",
        "method_function",
        "prepend_features",
        "prepended",
        "private",
        "refine",
        "remove_const",
        "remove_method",
        "undef_method",
        "using",
    ])

    BLACKLISTED_REPL_METHODS = Set.new([
        "pry",
        "binding",
        "__binding__",
        "remote_pry",
        "remote_pray",
        "pry_remote",
        "pray_remote"
    ])

    def kill_instance_methods(obj, msg)
      if [ "object_id", "__id__" ].include?(msg)
        return
      end

      if obj.is_a?(DRabUndumped)
        if obj.class.class_variable_get(:@@drab_whitelist).include?("to_s")
          if msg === "respond_to?"
            return
          end
        end
        if obj.class.class_variable_get(:@@drab_whitelist).include?(msg)
          return
        else
          puts "unwhitelisted method call attempted: #{msg} on" + obj.class.to_s
          raise Exception.new("unwhitelisted method call attempted")
        end
      end
      
      if BLACKLISTED_BASIC_OBJECT_METHODS.include?(msg)
        puts "dangerous BasicObject method called: #{msg} on " + obj.class.to_s
        raise Exception.new("dangerous BasicObject method called")
      end

      if BLACKLISTED_MODULE_METHODS.include?(msg)
        puts "dangerous Module method called: #{msg} on " + obj.class.to_s
        raise Exception.new("dangerous Module method called")
      end

      if BLACKLISTED_REPL_METHODS.include?(msg)
        puts "dangerous repl method called: #{msg} on " + obj.class.to_s
        raise Exception.new("dangerous repl method called")
      end

    end

    # jtd: adding symkey auth here

    def send_secret(stream)
      if DRab.current_server.secret != nil
        shared_secret = DRab.current_server.secret
        stream.write([shared_secret.size].pack('N') + shared_secret)
      end
    end

    # taken from rails/ActiveSupport/MessageVerifier/secure_compare
    def secure_compare(a, b)
      return false unless a.bytesize == b.bytesize

      l = a.unpack "C#{a.bytesize}"

      res = 0
      b.each_byte { |byte| res |= byte ^ l.shift }
      res == 0
    end

    def recv_secret(stream)
      if DRab.current_server.secret != nil
        shared_secret = DRab.current_server.secret
        received_secret = nil

        begin
          sz = stream.read(4)
        rescue
          raise(DRabConnError, $!.message, $!.backtrace)
        end
        raise(DRabConnError, 'connection closed') if sz.nil?
        raise(DRabConnError, 'secret size wrong size') if sz.size != 4
        sz = sz.unpack('N')[0]
        raise(DRabConnError, 'secret size wrong') if sz != shared_secret.size
        begin
          received_secret = stream.read(sz)
        rescue
          raise(DRabConnError, $!.message, $!.backtrace)
        end
        raise(DRabConnError, 'connection closed') if received_secret.nil?
        raise(DRabConnError, 'premature secret (can\'t read)') if received_secret.size < sz

        if received_secret.length != shared_secret.length
          stream.close
        elsif !secure_compare(shared_secret, received_secret)
          stream.close
        end
      end
    end
    # /jtd

    def recv_request(stream)
      recv_secret(stream)
      ref = load(stream, true)
      if not ref.is_a? Integer and not ref.is_a? NilClass
        raise Exception.new "non-numeric ref id"
      end
      ro = DRab.to_obj(ref)

      msg = load(stream, true)
      if not msg.is_a? String
        raise Exception.new "non-string msg name"
      end

      #note: obviously, blacklisting a few things is not enough, we also need
      #      to make sure that arbitrary objects can't just be wrapped by id
      #      or deserialized, and maybe do to limit the sorts of
      #      objects that can be passed to only primative or "DRab-aware" types.
      kill_instance_methods(ro, msg)

      argc = load(stream, true)
      if not argc.is_a? Integer
        raise Exception.new "non-numeric argc"
      end
      raise(DRabConnError, "too many arguments") if @argc_limit < argc
      argv = Array.new(argc, nil)
      argc.times do |n|
        argv[n] = load(stream)
      end
      block = nil

      return ro, msg, argv, block
    end

    def send_reply(stream, succ, result)
      send_secret(stream)
      stream.write(dump(succ) + dump(result, !succ))
    rescue
      raise(DRabConnError, $!.message, $!.backtrace)
    end

    def recv_reply(stream)
      recv_secret(stream)
      succ = load(stream)
      result = load(stream)
      if result.is_a?(DRabArray::Token)
        r = result.size.times.collect {|| load(stream) }
        [succ, r]
      else
        [succ, result]
      end
    end

    private
    def make_proxy(obj, error=false)
      if error
        DRabRemoteError.new(obj)
      else
        DRabObject.new(obj)
      end
    end
  end

  module DRabProtocol

    def add_protocol(prot)
      @protocol.push(prot)
    end
    module_function :add_protocol

    def open(uri, config, first=true)
      @protocol.each do |prot|
        begin
          return prot.open(uri, config)
        rescue DRabBadScheme
        rescue DRabConnError
          raise($!)
        rescue
          raise(DRabConnError, "#{uri} - #{$!.inspect}")
        end
      end
      if first && (config[:auto_load] != false)
        auto_load(uri)
        return open(uri, config, false)
      end
      raise DRabBadURI, 'can\'t parse uri:' + uri
    end
    module_function :open

    def open_server(uri, config, first=true)
      @protocol.each do |prot|
        begin
          return prot.open_server(uri, config)
        rescue DRabBadScheme
        end
      end
      if first && (config[:auto_load] != false)
        auto_load(uri)
        return open_server(uri, config, false)
      end
      raise DRabBadURI, 'can\'t parse uri:' + uri
    end
    module_function :open_server

    def uri_option(uri, config, first=true)
      @protocol.each do |prot|
        begin
          uri, opt = prot.uri_option(uri, config)
          # opt = nil if opt == ''
          return uri, opt
        rescue DRabBadScheme
        end
      end
      if first && (config[:auto_load] != false)
        auto_load(uri)
        return uri_option(uri, config, false)
      end
      raise DRabBadURI, 'can\'t parse uri:' + uri
    end
    module_function :uri_option

    def auto_load(uri)  # :nodoc:
      if uri =~ /^drab([a-z0-9]+):/
        require("drab/#{$1}") rescue nil
      end
    end
    module_function :auto_load
  end

  class DRabTCPSocket
    private
    def self.parse_uri(uri)
      if uri =~ /^druby:\/\/(.*?):(\d+)(\?(.*))?$/
        host = $1
        port = $2.to_i
        option = $4
        [host, port, option]
      else
        raise(DRabBadScheme, uri) unless uri =~ /^druby:/
        raise(DRabBadURI, 'can\'t parse uri:' + uri)
      end
    end

    public

    def self.open(uri, config)
      host, port, = parse_uri(uri)
      host.untaint
      port.untaint
      soc = TCPSocket.open(host, port)
      self.new(uri, soc, config)
    end

    def self.getservername
      host = Socket::gethostname
      begin
        Socket::gethostbyname(host)[0]
      rescue
        'localhost'
      end
    end

    def self.open_server_inaddr_any(host, port)
      infos = Socket::getaddrinfo(host, nil,
                                  Socket::AF_UNSPEC,
                                  Socket::SOCK_STREAM,
                                  0,
                                  Socket::AI_PASSIVE)
      families = Hash[*infos.collect { |af, *_| af }.uniq.zip([]).flatten]
      return TCPServer.open('0.0.0.0', port) if families.has_key?('AF_INET')
      return TCPServer.open('::', port) if families.has_key?('AF_INET6')
      return TCPServer.open(port)
    end

    def self.open_server(uri, config)
      uri = 'druby://:0' unless uri
      host, port, _ = parse_uri(uri)
      config = {:tcp_original_host => host}.update(config)
      if host.size == 0
        host = getservername
        soc = open_server_inaddr_any(host, port)
      else
        soc = TCPServer.open(host, port)
      end
      port = soc.addr[1] if port == 0
      config[:tcp_port] = port
      uri = "druby://#{host}:#{port}"
      self.new(uri, soc, config)
    end

    def self.uri_option(uri, config)
      host, port, option = parse_uri(uri)
      return "druby://#{host}:#{port}", option
    end

    def initialize(uri, soc, config={})
      @uri = uri
      @socket = soc
      @config = config
      @acl = config[:tcp_acl]
      @msg = DRabMessage.new(config)
      set_sockopt(@socket)
      @shutdown_pipe_r, @shutdown_pipe_w = IO.pipe
    end

    attr_reader :uri

    def peeraddr
      @socket.peeraddr
    end

    def stream; @socket; end

    def send_request(ref, msg_id, arg, b)
      @msg.send_request(stream, ref, msg_id, arg, b)
    end

    def recv_request
      @msg.recv_request(stream)
    end

    def send_reply(succ, result)
      @msg.send_reply(stream, succ, result)
    end

    def recv_reply
      @msg.recv_reply(stream)
    end

    public

    def close
      if @socket
        @socket.close
        @socket = nil
      end
      close_shutdown_pipe
    end

    def close_shutdown_pipe
      if @shutdown_pipe_r && !@shutdown_pipe_r.closed?
        @shutdown_pipe_r.close
        @shutdown_pipe_r = nil
      end
      if @shutdown_pipe_w && !@shutdown_pipe_w.closed?
        @shutdown_pipe_w.close
        @shutdown_pipe_w = nil
      end
    end
    private :close_shutdown_pipe

    def accept
      while true
        s = accept_or_shutdown
        return nil unless s
        break if (@acl ? @acl.allow_socket?(s) : true)
        s.close
      end
      if @config[:tcp_original_host].to_s.size == 0
        uri = "druby://#{s.addr[3]}:#{@config[:tcp_port]}"
      else
        uri = @uri
      end

      self.class.new(uri, s, @config)
    end

    def accept_or_shutdown
      readables, = IO.select([@socket, @shutdown_pipe_r])
      if readables.include? @shutdown_pipe_r
        return nil
      end
      @socket.accept
    end
    private :accept_or_shutdown

    def shutdown
      @shutdown_pipe_w.close if @shutdown_pipe_w && !@shutdown_pipe_w.closed?
    end

    def alive?
      return false unless @socket
      if IO.select([@socket], nil, nil, 0)
      #if @socket.to_io.wait_readable(0)
        close
        return false
      end
      true
    end

    def set_sockopt(soc)
      soc.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    end
  end

  module DRabProtocol
    @protocol = [DRabTCPSocket] # default
  end

#  class DRabURIOption  # :nodoc:  I don't understand the purpose of this class... # <-- which is why it shouldn't exist
#    def initialize(option)
#      @option = option.to_s
#    end
#    attr_reader :option
#    def to_s; @option; end
#
#    def ==(other)
#      return false unless DRabURIOption === other
#      @option == other.option
#    end
#
#    def hash
#      @option.hash
#    end
#
#    alias eql? ==
#  end

  class DRabObject
    def self._load(s)
      #uri, ref = Marshal::load(s)
      uri, ref = JSON::load(s)

      if DRab.here?(uri)
        obj = DRab.to_obj(ref)
        if ((! obj.tainted?) && Thread.current[:drab_untaint])
          Thread.current[:drab_untaint].push(obj)
        end
        return obj
      end

      self.new_with(uri, ref)
    end

    def self.new_with(uri, ref)
      it = self.allocate
      it.instance_variable_set(:@uri, uri)
      it.instance_variable_set(:@ref, ref)
      it
    end

    def self.new_with_uri(uri)
      self.new(nil, uri)
    end

    def _dump(lv)
      #Marshal::dump([@uri, @ref])
      JSON::dump([@uri, @ref])
    end

    def initialize(obj, uri=nil)
      @uri = nil
      @ref = nil
      if obj.nil?
        return if uri.nil?
        @uri, option = DRabProtocol.uri_option(uri, DRab.config)
        #@ref = DRabURIOption.new(option) unless option.nil?
        @ref = option.to_s unless option.nil?
      else
        @uri = uri ? uri : (DRab.uri rescue nil)
        @ref = obj ? DRab.to_id(obj) : nil
      end
    end

    def __draburi
      @uri
    end

    def __drabref
      @ref
    end

    undef :to_s
    undef :to_a if respond_to?(:to_a)

    def respond_to?(msg_id, priv=false)
      case msg_id
      when :_dump
        true
      when :marshal_dump
        false
      else
        method_missing(:respond_to?, msg_id, priv)
      end
    end

    def method_missing(msg_id, *a, &b)
      if DRab.here?(@uri)
        obj = DRab.to_obj(@ref)
        DRab.current_server.check_insecure_method(obj, msg_id)
        return obj.__send__(msg_id, *a, &b)
      end

      succ, result = self.class.with_friend(@uri) do
        DRabConn.open(@uri) do |conn|
          suc, res = conn.send_message(self, msg_id, a, b)
        end
      end

      if succ
        return result
      elsif DRabUnknown === result
        raise result
      else
        bt = self.class.prepare_backtrace(@uri, result)
        result.set_backtrace(bt + caller)
        raise result
      end
    end

    def self.with_friend(uri)
      friend = DRab.fetch_server(uri)
      return yield() unless friend

      save = Thread.current['DRab']
      Thread.current['DRab'] = { 'server' => friend }
      return yield
    ensure
      Thread.current['DRab'] = save if friend
    end

    def self.prepare_backtrace(uri, result)
      prefix = "(#{uri}) "
      bt = []
      result.backtrace.each do |x|
        break if /`__send__'$/ =~ x
        if /^\(druby:\/\// =~ x
          bt.push(x)
        else
          bt.push(prefix + x)
        end
      end
      bt
    end

    def pretty_print(q)
      q.pp_object(self)
    end

    def pretty_print_cycle(q)
      q.object_address_group(self) {
        q.breakable
        q.text '...'
      }
    end
  end

  class DRabConn
    POOL_SIZE = 16
    #@mutex = Thread::Mutex.new
    @mutex = Mutex.new
    @pool = []

    def self.open(remote_uri)
      begin
        conn = nil

        @mutex.synchronize do
          #FIXME
          new_pool = []
          @pool.each do |c|
            if conn.nil? and c.uri == remote_uri
              conn = c if c.alive?
            else
              new_pool.push c
            end
          end
          @pool = new_pool
        end

        conn = self.new(remote_uri) unless conn
        succ, result = yield(conn)
        return succ, result

      ensure
        if conn
          if succ
            @mutex.synchronize do
              @pool.unshift(conn)
              @pool.pop.close while @pool.size > POOL_SIZE
            end
          else
            conn.close
          end
        end
      end
    end

    def initialize(remote_uri)
      @uri = remote_uri
      @protocol = DRabProtocol.open(remote_uri, DRab.config)
    end
    attr_reader :uri

    def send_message(ref, msg_id, arg, block)
      @protocol.send_request(ref, msg_id, arg, block)
      @protocol.recv_reply
    end

    def close
      @protocol.close
      @protocol = nil
    end

    def alive?
      return false unless @protocol
      @protocol.alive?
    end
  end

  class DRabServer
    @@acl = nil
    @@idconv = DRabIdConv.new
    @@secondary_server = nil
    @@argc_limit = 256
    @@load_limit = 256 * 102400
    @@verbose = false
    @@safe_level = 0

    def self.default_argc_limit(argc)
      @@argc_limit = argc
    end

    def self.default_load_limit(sz)
      @@load_limit = sz
    end

    def self.default_acl(acl)
      @@acl = acl
    end

    def self.default_safe_level(level)
      @@safe_level = level
    end

    def self.verbose=(on)
      @@verbose = on
    end

    def self.verbose
      @@verbose
    end

    def self.make_config(hash={})
      default_config = {
        :verbose => @@verbose,
        :tcp_acl => @@acl,
        :load_limit => @@load_limit,
        :argc_limit => @@argc_limit,
        :safe_level => @@safe_level,
        :secret => nil
      }
      default_config.update(hash)
    end

    def initialize(uri=nil, front=nil, config_or_acl=nil)
      if Hash === config_or_acl
        config = config_or_acl.dup
      else
        acl = config_or_acl || @@acl
        config = {
          :tcp_acl => acl
        }
      end

      @config = self.class.make_config(config)

      @protocol = DRabProtocol.open_server(uri, @config)
      @uri = @protocol.uri
      @exported_uri = [@uri]
      @secret = @config[:secret]

      @front = front
      @idconv = @@idconv
      @safe_level = @config[:safe_level]

      @grp = ThreadGroup.new
      @thread = run

      DRab.regist_server(self)
    end

    attr_reader :uri

    attr_reader :secret

    attr_reader :thread

    attr_reader :front

    attr_reader :config

    attr_reader :safe_level

    def verbose=(v); @config[:verbose]=v; end

    def verbose; @config[:verbose]; end

    def alive?
      @thread.alive?
    end

    def here?(uri)
      @exported_uri.include?(uri)
    end

    def stop_service
      DRab.remove_server(self)
      if  Thread.current['DRab'] && Thread.current['DRab']['server'] == self
        Thread.current['DRab']['stop_service'] = true
      else
        if @protocol.respond_to? :shutdown
          @protocol.shutdown
        else
          [@thread, *@grp.list].each {|thread| thread.kill}
        end
        @thread.join
      end
    end

    def to_obj(ref)
      return front if ref.nil?
      #return front[ref.to_s] if DRabURIOption === ref
      return front[ref] if ref.is_a? String
      @idconv.to_obj(ref)
    end

    def to_id(obj)
      return nil if obj.__id__ == front.__id__
      @idconv.to_id(obj)
    end


    private

    def run
      Thread.start do
        begin
          while main_loop
          end
        ensure
          @protocol.close if @protocol
        end
      end
    end

    # List of insecure methods. # it's a list of "insecure method" (singular)
    #
    # These methods are not callable via dRuby. # you say that, but :send was allowed. that and a bunch of other dangerous stuff.
    INSECURE_METHOD = [
      :__send__
    ]

    def insecure_method?(msg_id)
      INSECURE_METHOD.include?(msg_id)
    end

    def any_to_s(obj)
      obj.to_s + ":#{obj.class}"
    rescue
      sprintf("#<%s:0x%lx>", obj.class, obj.__id__)
    end

    def check_insecure_method(obj, msg_id)
      return true if Proc === obj && msg_id == :__drab_yield
      raise(ArgumentError, "#{any_to_s(msg_id)} is not a symbol") unless Symbol == msg_id.class
      raise(SecurityError, "insecure method `#{msg_id}'") if insecure_method?(msg_id)

      if obj.private_methods.include?(msg_id)
        desc = any_to_s(obj)
        raise NoMethodError, "private method `#{msg_id}' called for #{desc}"
      elsif obj.protected_methods.include?(msg_id)
        desc = any_to_s(obj)
        raise NoMethodError, "protected method `#{msg_id}' called for #{desc}"
      else
        true
      end
    end
    public :check_insecure_method

    class InvokeMethod
      def initialize(drab_server, client)
        @drab_server = drab_server
        @safe_level = drab_server.safe_level
        @client = client
      end

      def perform
        @result = nil
        @succ = false
        setup_message

        if $SAFE < @safe_level
          info = Thread.current['DRab']
          @result = Thread.new {
            Thread.current['DRab'] = info
            $SAFE = @safe_level
            perform_without_block
          }.value
        else
          @result = perform_without_block
        end
        @succ = true
        if @result.class == Array
          @result = DRabArray.new(@result)
        end
        return @succ, @result
      rescue StandardError, ScriptError, Interrupt
        @result = $!
        return @succ, @result
      end

      private
      def init_with_client
        obj, msg, argv, block = @client.recv_request
        @obj = obj
        @msg_id = msg.intern
        @argv = argv
        @block = block
      end

      def check_insecure_method
        @drab_server.check_insecure_method(@obj, @msg_id)
      end

      def setup_message
        init_with_client
        check_insecure_method
      end

      def perform_without_block
        #STDOUT.puts @obj.class.to_s + "::" + @msg_id.to_s
        if Proc === @obj && @msg_id == :__drab_yield
          if @argv.size == 1
            ary = @argv
          else
            ary = [@argv]
          end
          ary.collect(&@obj)[0]
        elsif Proc === @obj and @msg_id != :call
          STDERR.puts "spooky proc call of " + @msg_id.to_s
          raise Exception.new "spooky proc call of " + @msg_id.to_s
        else
          if @obj.class.method_defined?(@msg_id)
            @obj.__send__(@msg_id, *@argv)
          else
            STDERR.puts "spooky call of " + @msg_id.to_s + " on " + @obj.class.to_s
            raise Exception.new "spooky call of " + @msg_id.to_s + " on " + @obj.class.to_s
          end
        end
      end
    end

    require 'drab/invokemethod'
    class InvokeMethod
      include InvokeMethod18Mixin
    end

    def error_print(exception)
      exception.backtrace.inject(true) do |first, x|
        if first
          STDERR.puts "#{x}: #{exception} (#{exception.class})"
        else
          STDERR.puts "\tfrom #{x}"
        end
        false
      end
    end

    def main_loop
      client0 = @protocol.accept
      return nil if !client0
      Thread.start(client0) do |client|
        @grp.add Thread.current
        Thread.current['DRab'] = { 'client' => client ,
                                  'server' => self }
        DRab.mutex.synchronize do
          client_uri = client.uri
          @exported_uri << client_uri unless @exported_uri.include?(client_uri)
        end
        loop do
          begin
            succ = false
            invoke_method = InvokeMethod.new(self, client)
            succ, result = invoke_method.perform
            error_print(result) if !succ && verbose
            client.send_reply(succ, result)
          rescue Exception => e
            error_print(e) if verbose
          ensure
            client.close unless succ
            if Thread.current['DRab']['stop_service']
              Thread.new { stop_service }
            end
            break unless succ
          end
        end
      end
    end
  end

  @primary_server = nil

  def start_service(uri=nil, front=nil, config=nil)
    if config == nil
      config = {}
    end
    if uri.include? "?secret="
      uri, secret = uri.split("?secret=")
      config[:secret] = secret
    end

    @primary_server = DRabServer.new(uri, front, config)
  end
  module_function :start_service

  attr_accessor :primary_server
  module_function :primary_server=, :primary_server

  def current_server
    drab = Thread.current['DRab']
    server = (drab && drab['server']) ? drab['server'] : @primary_server
    raise DRabServerNotFound unless server
    return server
  end
  module_function :current_server

  def stop_service
    @primary_server.stop_service if @primary_server
    @primary_server = nil
  end
  module_function :stop_service

  def uri
    drab = Thread.current['DRab']
    client = (drab && drab['client'])
    if client
      uri = client.uri
      return uri if uri
    end
    current_server.uri
  end
  module_function :uri

  def here?(uri)
    current_server.here?(uri) rescue false
    # (current_server.uri rescue nil) == uri
  end
  module_function :here?

  def config
    current_server.config
  rescue
    DRabServer.make_config
  end
  module_function :config

  def front
    current_server.front
  end
  module_function :front

  def to_obj(ref)
    current_server.to_obj(ref)
  end

  def to_id(obj)
    current_server.to_id(obj)
  end
  module_function :to_id
  module_function :to_obj

  def thread
    @primary_server ? @primary_server.thread : nil
  end
  module_function :thread

  def install_acl(acl)
    DRabServer.default_acl(acl)
  end
  module_function :install_acl

  #@mutex = Thread::Mutex.new
  @mutex = Mutex.new
  def mutex
    @mutex
  end
  module_function :mutex

  @server = {}
  def regist_server(server)
    @server[server.uri] = server
    mutex.synchronize do
      @primary_server = server unless @primary_server
    end
  end
  module_function :regist_server

  def remove_server(server)
    @server.delete(server.uri)
  end
  module_function :remove_server

  def fetch_server(uri)
    @server[uri]
  end
  module_function :fetch_server
end

DRabObject = DRab::DRabObject
DRabUndumped = DRab::DRabUndumped
DRabIdConv = DRab::DRabIdConv
