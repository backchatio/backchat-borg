# -*- encoding: utf-8 -*-

module Backchat
  module Borg
    class ZMessage

      DELIMITER = ""

      def self.read(socket)
        parts = []
        socket.recv_strings parts
        ZMessage.new(*parts)
      end

      def initialize(*args)
        @parts = args
      end

      def size 
        @parts.size
      end

      def address=(addr)
        append addr
      end

      def address
        @parts.first || DELIMITER
      end

      def body
        @parts.last || DELIMITER
      end

      def body=(bd)
        @parts = @parts[0..-2]
        append bd
      end

      def send_to(socket)
        sender = socket.respond_to?(:raw_socket) ? socket.raw_socket : socket
        rc = 0
        @parts[0..-2].each { |part|
          rc = sender.send_string part, ZMQ::SNDMORE
        }
        rc = sender.send_string @parts[-1], 0
      end

      def to_s
        "ZMessage(#{@parts.map { |part| "'#{part}'" }.join(", ")})"
      end

      def append(data)
        @parts.push data
        self
      end

      def push(data)
        @parts.unshift data
        self
      end

      def pop
        @parts.shift || DELIMITER
      end

      def ccid
        get(5)
      end

      def ccid=(mt)
        set(5, mt)
      end

      def target 
        get(2)
      end

      def target=(mt)
        set(2, mt)
      end

      def sender
        get(3)
      end

      def sender=(mt)
        set(3, mt)
      end

      def message_type
        get(4)
      end

      def message_type=(mt) 
        set(4, mt)
      end

      def addresses
        (size > 5 ? @parts[0...-5].reject { |p| p.empty? } : []).flatten
      end

      def addresses=(*addr) 
        @parts = size > 5 ? @parts[0...5] : @parts
        @parts.unshift DELIMITER
        @parts = addr + @parts
        self
      end

      def unwrap
        addr = @parts.shift
        addr = @parts.shift if addr.empty?
        if (@parts.first||"").empty?
          @parts.shift # also remove the delimiter in one go
        end
        addr
      end

      def wrap(*pts) 
        @parts = pts + @parts
        self
      end

      def clone
        ZMessage.new(*@parts.clone)
      end

      private
      def get(index)
        @parts.size >= index ? @parts[size - index] || DELIMITER : DELIMITER
      end

      def set(index, value)
        if (size > (index - 1))
          @parts.delete_at(size - index)
          @parts.insert(size - (index - 1), value)
        else 
          push(value)
        end
      end
    end
  end
end
