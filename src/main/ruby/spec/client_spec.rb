# coding: utf-8

require File.dirname(__FILE__) + "/spec_helper"

describe Backchat::Borg::Client do
  include_context "zeromq_context"

  it "can enqueue a message to the server" do
    name = "zeromq-client-test"
    madeit = nil
    create_server(:name => name)
    client = Backchat::Borg::Client.new(:server => "tcp://127.0.0.1:#{@server.port}" , :id => "#{name}-client")
    begin
      client.tell "blah", "yada"
      @message_received.wait
      @server.last_message.size.should eql(7)
    ensure
      client.disconnect
    end
  end

  it "can request a message with a reply" do
    name = "zeromq-client-reply-test"
    create_server :name => name
    client = Backchat::Borg::Client.new(:server => "tcp://127.0.0.1:#{@server.port}" , :id => "#{name}-client")
    begin
      client.ask name, { :some => "seed" } do |evt, data|
        data.should eql("the data")
      end
    ensure
      client.disconnect
    end
  end

  it "should not block forever if a request doesn't get a reply" do
    name = "zeromq-client-noreply-test"
    create_server :name => name, :reply => false 
    client = Backchat::Borg::Client.new(:server => "tcp://127.0.0.1:#{@server.port}" , :id => "#{name}-client")
    begin
      expect { 
        client.ask(name, ["dontreply", { :some => "seed" }]) 
      }.to raise_error(RequestTimeoutException)
    ensure
      client.disconnect
    end
  end

  it "throws a ServerUnavailableException when the backend responds with that" do
    name = "zeromq-client-unavailable-test"
    create_server :name => name
    client = Backchat::Borg::Client.new(:server => "tcp://127.0.0.1:#{@server.port}" , :id => "#{name}-client")
    begin
      expect { client.ask(name, ["server_unavailable", { :some => "seed" }]) }.to raise_error(ServerUnavailableException)
    ensure
      client.disconnect
    end
  end

  it "throws a RequestTimeoutException when the backend responds with that" do
    name = "zeromq-client-timeout-test"
    create_server :name => name
    client = Backchat::Borg::Client.new(:server => "tcp://127.0.0.1:#{@server.port}" , :id => "#{name}-client")
    begin
      expect { client.ask(name, ["timeout", { :some => "seed" }]) }.to raise_error(RequestTimeoutException)
    ensure
      client.disconnect
    end
  end
end

# vim: set si ts=2 sw=2 sts=2 et:

