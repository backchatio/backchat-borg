require 'active_support'
require 'zmq'
require 'uuidtools'
require 'logger'
require 'backchat_borg/zmessage'

module Backchat
  module Borg

    def self.logger=(new_logger)
      @@logger = new_logger
    end

    def self.logger 
      @@logger
    end
    
  end
end

include Backchat::Borg