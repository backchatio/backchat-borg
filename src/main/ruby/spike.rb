# -*- encoding: utf-8 -*-

puts "Starting spike"
$:.unshift File.expand_path(File.dirname(__FILE__) + "/lib")
require 'backchat_borg'

puts "Creating client"
client = Backchat::Borg::Client.new(:id => "ruby-test-client", :server => "tcp://127.0.0.1:13242")
client.tell "the-target", '["pong"]'
puts "sent message"
