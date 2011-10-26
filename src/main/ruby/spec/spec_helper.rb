require 'backchat_borg'

Backchat::Borg.logger = Logger.new("/dev/null")

class TestServer 
  def initialize(context, name, &block)
    @callback = lambda { |msg| block.call(self, msg) } if block
    @router = context.socket(ZMQ::ROUTER)
    @router.bind "inproc://#{name}.inproc"
    @poller = ZMQ::Poller.new
    @poller.register_readable @router
  end
  
  def poll
    @poller.poll
    msg = ZMessage.read @router
    @callback ? @callback.call(msg) : msg
  end

  def send(msg)
    @router.send msg
  end

  def close
    @poller.deregister_readable @router
    @router.close
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
    @server.close if @server
  end

  def create_server(name, &callback)
    @server = TestServer.new @zmq, name, &callback
  end

end

