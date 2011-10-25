# -*- encoding: utf-8 -*-

require "zmq"
require "uuidtools"

class ZMessage

  DELIMITER = ""

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
    @parts[0..-2].each { |part|
      socket.send part, ZMQ::SNDMORE
    }
    socket.send @parts[-1], 0
  end

  def to_s
    "ZMessage(#{parts.map { |part| "\"#{part}\"" }.join(", ")})"
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

  def messageType
    get(4)
  end

  def messageType=(mt) 
    set(4, mt)
  end

  def addresses
    size > 5 ? @parts.drop_right(5).reject { |p| p.empty? } : []
  end

  def addresses=(*addr) 
    @parts = size > 5 ? @parts.take_right(5) : @parts
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

context = ZMQ::Context.new(1)

outbound = context.socket(ZMQ::DEALER)
outbound.setsockopt ZMQ::LINGER, 0
outbound.setsockopt ZMQ::IDENTITY, "ruby-test-client"
outbound.connect("tcp://127.0.0.1:13242")
ZMessage.new("", UUIDTools::UUID.random_create.to_s, "fireforget", "", "the-target", '["pong"]').send_to outbound

