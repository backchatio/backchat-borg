# coding: utf-8
require File.dirname(__FILE__) + "/spec_helper"

describe Backchat::Borg::Client do
  include_context "zeromq_context"

  it "can enqueue a message to the server" do
    name = "zeromq-client-test"
    create_server name
    client = Backchat::Borg::Client.new(:server => "inproc://#{name}.inproc" , :id => "#{name}-client")
    begin
      client.tell "blah", "yada"
      @server.poll.size.should eql(7)
    ensure
      client.disconnect
    end
  end

  it "can request a message with a reply" do
    name = "zeromq-client-reply-test"
    

    #val latch = new StandardLatch
    #val router = ZeroMQ startDevice {
      #new BackchatZeroMqDevice(config) {
        #val sock = context.socket(Router)
        #poller += (sock -> (send _))
        #override def send(zmsg: ZMessage) {
          #if (zmsg.messageType == "requestreply" &&
            #zmsg.sender == (name + "-client") &&
            #zmsg.body == ApplicationEvent('pingping).toJson) {
            #zmsg(sock)
          #}
        #}
        #override def init() {
          #super.init()
          #sock.bind("inproc://" + name + ".inproc")
          #latch.open()
        #}
        #override def dispose() {
          #sock.close()
          #super.dispose()
        #}
      #}
    #}
    #latch.tryAwait(2, TimeUnit.SECONDS) must be(true)
    #val client = new BackchatZeroMqClient(name + "-client", context, name)
    #val replyLatch = new StandardLatch
    #client.request("the-target", ApplicationEvent('pingping)) { evt ⇒
      #evt must equal(ApplicationEvent('pingping))
      #replyLatch.open()
    #}
    #replyLatch.tryAwait(2, TimeUnit.SECONDS) must be(true)
    #router.stop
  end

  it "should not block forever if a request doesn't get a reply" do
    pending
  end

  it "throws a ServerUnavailableException when the backend responds with that" do
    pending
  end

  it "throws a RequestTimeoutException when the backend responds with that" do
    pending 
  end
end

# vim: set si ts=2 sw=2 sts=2 et:
