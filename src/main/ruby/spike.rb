# -*- encoding: utf-8 -*-

puts "Starting spike"
$:.unshift File.expand_path(File.dirname(__FILE__) + "/lib")
require 'backchat_borg'

puts "Creating client"
client = Backchat::Borg::Client.new(:id => "ruby-test-client", :server => "tcp://127.0.0.1:13333")
begin
  client.tell "the-target", '["pong"]'
  client.ask "the-target", ["a_request", "a param"] do |evt, data|
    puts "event: #{evt}"
    puts "data: #{data}"
  end
  puts "sent message"
ensure
  client.disconnect
end
