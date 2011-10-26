# -*- encoding: utf-8 -*-

require 'rubygems'
unless RUBY_PLATFORM == "java"
  require 'yajl'
else
  require 'json'
end
require 'active_support'
require 'ffi-rzmq'
require 'uuidtools'
require 'logger'
require 'backchat_borg/zmessage'
require 'backchat_borg/error'
require 'backchat_borg/client'

module Backchat
  module Borg

    def self.logger=(new_logger)
      @@logger = new_logger
    end

    def self.logger 
      @@logger
    end

    def self.context
      @@context ||= ZMQ::Context.new(1)
    end

    def self.context=(ctxt)
      unless @@context.nil?
        @@context.terminate
      end
      @@context = ctxt
    end
    
  end
end

include Backchat::Borg
