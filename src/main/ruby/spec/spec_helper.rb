require 'thread'
require 'backchat_borg'

class CountDownLatch
  attr_reader :count

  def initialize(to)
    @count = to.to_i
    raise ArgumentError, "cannot count down from negative integer" unless @count >= 0
    @lock = Mutex.new
    @condition = ConditionVariable.new
  end

  def count_down
    @lock.synchronize do
      @count -= 1 if @count > 0
      @condition.broadcast if @count == 0
    end
  end

  def wait
    @lock.synchronize do
      @condition.wait(@lock) while @count > 0
    end
  end
end

class StandardLatch
  attr_reader :is_open

  def initialize(is_open=false)
    @is_open = is_open
    @original = is_open
    @lock = Mutex.new
    @condition = ConditionVariable.new
  end

  def open!
    @lock.synchronize do
      @is_open = !is_open
      @condition.broadcast 
    end
  end

  def wait
    @lock.synchronize do
      @condition.wait(@lock) while @original == @is_open
    end
  end
end



Backchat::Borg.logger = Logger.new("/dev/null")

class ServerHandler 

  attr_reader :port, :last_message

  def initialize context, opts = {}, &callback
    @context = context
    @started = opts[:started_latch]
    @send_reply = opts[:reply].nil? ? true : opts[:reply]
    @message_received = opts[:message_received_latch]
    @callback = lambda { |msg| callback.call(self, msg) } if callback
  end

  def on_attach socket
    @port = bind_to_random_tcp_port socket
    @started.open!
    @port
  end

  def on_readable socket, messages
    parts = messages.map { |msg| msg.copy_out_string }
    @last_message = ZMessage.new(*parts)
    @callback.call msg if @callback
    if should_reply
      repl = reply(last_message, ["event_name", "the data"])
      rc = repl.send_to socket 
    end
    @message_received.open!
    rc
  end

  private

  def should_reply
    last_message.message_type == "requestreply" && @send_reply
  end

  def reply(msg, data)
    decoded = ActiveSupport::JSON.decode(msg.body)
    r = msg.clone
    case decoded.first
    when "server_unavailable"
      r.target = r.sender
      r.sender = "ERROR"
      r.message_type = "system"
      r.body = "SERVER_UNAVAILABLE"
    when "timeout"
      r.target = r.sender
      r.sender = "ERROR"
      r.message_type = "system"
      r.body = "TIMEOUT"
    else
      r.target = r.sender
      r.sender = nil
      r.body = data.is_a?(String) ? data : data.to_json
    end
    r
  end

  # generate a random port between 10_000 and 65534
  def random_port
    rand(55534) + 10_000
  end

  def bind_to_random_tcp_port socket, max_tries = 500
    tries = 0
    rc = -1

    while !ZMQ::Util.resultcode_ok?(rc) && tries < max_tries
      tries += 1
      random = random_port
      rc = socket.bind ZM::Address.create("127.0.0.1", random, :tcp)
    end

    random
  end
end

RSpec.configure do |c|

end

shared_context "zeromq_context" do

  before(:all) do
    @zmq = Backchat::Borg.context
  end

  after(:all) do
    Backchat::Borg.context = nil
  end

  after(:each) do
    if @serv_reactor
      @serv_reactor.stop 
      @serv_reactor.join 5
    end
  end

  def create_server(opts = {}, &callback)
    @started = StandardLatch.new
    @message_received = StandardLatch.new
    @serv_reactor = ZM::Reactor.new(:test).run do |context|
      @server = ServerHandler.new context, { :started_latch => @started, :message_received_latch => @message_received }.merge(opts), &callback
      context.xrep_socket @server
    end
    @started.wait
  end

end

