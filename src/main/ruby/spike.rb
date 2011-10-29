# -*- encoding: utf-8 -*-

puts "Starting spike"
$:.unshift File.expand_path(File.dirname(__FILE__) + "/lib")
require 'backchat_borg'

puts "Creating client"
client = Backchat::Borg::Client.new(:id => "ruby-test-client", :server => "tcp://127.0.0.1:13333")
begin
  client.tell "the-target", ["pong"]
  client.ask "the-target", ["a_request", "a param"] do |evt, data|
    puts "event: #{evt}"
    puts "data: #{data}"
  end
  puts "sent message"
ensure
  client.disconnect
end

#def create_server(name, &callback)
  ##@server = TestServer.new @zmq, name, &callback
  #@serv_reactor = ZM::Reactor.new(:test).run do |context|
    #@server = ServerHandler.new(context, name)
    #context.xrep_socket @server
    #puts "reactor is running"
  #end
#end

#begin
  #create_server("testing")
  #sleep 0.1
  #puts "Creating client"
  #client = Backchat::Borg::Client.new(:id => "ruby-test-client", :server => "tcp://127.0.0.1:38578")
  #begin
    #client.tell "the-target", ["pong"]
    #client.ask "the-target", ["a_request", "a param"] do |evt, data|
      #puts "event: #{evt}"
      #puts "data: #{data}"
    #end
    #res = client.ask "the-target", ["a_request", "a param"]
    #puts "got result: #{res.join(", ")}"
    #puts "sent message"
  #ensure
    #puts "disconnecting client"
    #client.disconnect
  #end
#ensure
  #@serv_reactor.stop
#end
#@serv_reactor.join
