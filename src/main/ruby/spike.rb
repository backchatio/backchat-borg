# -*- encoding: utf-8 -*-

$:.unshift File.expand_path(File.dirname(__FILE__) + "/lib")
require 'backchat_borg'

client = Backchat::Borg::Client.new(:id => "ruby-test-client", :server => "tcp://127.0.0.1:13242")
client.tell "the-target", '["pong"]'

