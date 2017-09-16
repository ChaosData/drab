# frozen_string_literal: false
require 'socket'
require 'drab/drab'
require 'tmpdir'

raise(LoadError, "UNIXServer is required") unless defined?(UNIXServer)

module DRab

  # Implements DRab over a UNIX socket
  #
  # DRab UNIX socket URIs look like <code>drabunix:<path>?<option></code>.  The
  # option is optional.

  class DRabUNIXSocket < DRabTCPSocket
    # :stopdoc:
    def self.parse_uri(uri)
      if /^drabunix:(.*?)(\?(.*))?$/ =~ uri
        filename = $1
        option = $3
        [filename, option]
      else
        raise(DRabBadScheme, uri) unless uri =~ /^drabunix:/
        raise(DRabBadURI, 'can\'t parse uri:' + uri)
      end
    end

    def self.open(uri, config)
      filename, = parse_uri(uri)
      filename.untaint
      soc = UNIXSocket.open(filename)
      self.new(uri, soc, config)
    end

    def self.open_server(uri, config)
      filename, = parse_uri(uri)
      if filename.size == 0
        soc = temp_server
        filename = soc.path
        uri = 'drabunix:' + soc.path
      else
        soc = UNIXServer.open(filename)
      end
      owner = config[:UNIXFileOwner]
      group = config[:UNIXFileGroup]
      if owner || group
        require 'etc'
        owner = Etc.getpwnam( owner ).uid  if owner
        group = Etc.getgrnam( group ).gid  if group
        File.chown owner, group, filename
      end
      mode = config[:UNIXFileMode]
      File.chmod(mode, filename) if mode

      self.new(uri, soc, config, true)
    end

    def self.uri_option(uri, config)
      filename, option = parse_uri(uri)
      return "drabunix:#{filename}", option
    end

    def initialize(uri, soc, config={}, server_mode = false)
      super(uri, soc, config)
      set_sockopt(@socket)
      @server_mode = server_mode
      @acl = nil
    end

    # import from tempfile.rb
    Max_try = 10
    private
    def self.temp_server
      tmpdir = Dir::tmpdir
      n = 0
      while true
        begin
          tmpname = sprintf('%s/druby%d.%d', tmpdir, $$, n)
          lock = tmpname + '.lock'
          unless File.exist?(tmpname) or File.exist?(lock)
            Dir.mkdir(lock)
            break
          end
        rescue
          raise "cannot generate tempfile `%s'" % tmpname if n >= Max_try
          #sleep(1)
        end
        n += 1
      end
      soc = UNIXServer.new(tmpname)
      Dir.rmdir(lock)
      soc
    end

    public
    def close
      return unless @socket
      path = @socket.path if @server_mode
      @socket.close
      File.unlink(path) if @server_mode
      @socket = nil
      close_shutdown_pipe
    end

    def accept
      s = accept_or_shutdown
      return nil unless s
      self.class.new(nil, s, @config)
    end

    def set_sockopt(soc)
      # no-op for now
    end
  end

  DRabProtocol.add_protocol(DRabUNIXSocket)
  # :startdoc:
end
