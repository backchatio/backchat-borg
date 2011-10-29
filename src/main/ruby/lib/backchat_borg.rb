# -*- encoding: utf-8 -*-

require 'rubygems'
unless RUBY_PLATFORM == "java"
  require 'yajl'
end
require 'active_support'
require 'active_support/core_ext'

require 'zmqmachine'
require 'uuidtools'
require 'logger'
require 'backchat_borg/zmessage'
require 'backchat_borg/error'
Thread.abort_on_exception = true

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

    #
    # Creates a new call control id
    #
    def self.new_ccid
      UUIDTools::UUID.random_create.to_s
    end
    
  end
end

include Backchat::Borg
require 'backchat_borg/client'

