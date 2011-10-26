# coding: utf-8
require File.dirname(__FILE__) + "/spec_helper"

describe Backchat::Borg::Client do
  include_context "zeromq_context"

  it "can enqueue a message to the server" do
    name = "zeromq-client-test"
    router = @zmq.socket(ZMQ::ROUTER)
    router.bind "inproc://#{name}.inproc"
    poller = ZMQ::Poller.new
    poller.register_readable router
    got_message = false
    client = Backchat::Borg::Client.new(:server => "inproc://#{name}.inproc" , :id => name)
    begin
      client.tell "blah", "yada"
      poller.poll
      m = []
      r = router.recv_strings m, ZMQ::NOBLOCK
      m.size.should eql(7)
      r.should eql(0)
    ensure
      client.disconnect
      poller.deregister_readable router
      router.close
    end
  end

  it "can request a message with a reply" do
    pending
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
