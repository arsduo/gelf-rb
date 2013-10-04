module GELF
  # Plain Ruby UDP sender.
  class RubyUdpSender
    attr_accessor :addresses

    def initialize(addresses)
      @addresses = addresses
      @i = 0
      @socket = UDPSocket.open
    end

    def send_datagrams(datagrams)
      host, port = @addresses[@i]
      @i = (@i + 1) % @addresses.length
      datagrams.each do |datagram|
        @socket.send(datagram, 0, host, port)
      end
    end

    def compress_message?
      false
    end
  end

  class RubyTcpSender
    attr_accessor :addresses

    def initialize(addresses)
      @i = 0
      self.addresses = addresses
    end

    def send_datagrams(datagrams)
      socket = @sockets[@i]
      @i = (@i + 1) % @sockets.length
      datagrams.each do |datagram|
        # \0 terminates each TCP frame on the connection
        socket.send "#{datagram}\0", 0
      end
    end

    def addresses=(new_addresses)
      @addresses = new_addresses
      @sockets = build_sockets
    end

    def compress_message?
      false
    end

    protected

    def build_sockets
      @addresses.map {|(host, port)| TCPSocket.new(host, port)}
    end
  end
end
