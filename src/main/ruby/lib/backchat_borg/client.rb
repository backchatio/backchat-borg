# coding: utf-8
# -*- encoding: utf-8 -*-

module Backchat
  module Borg

    class Client

      def initialize(config)
        @client = Backchat::Borg.context.socket(ZMQ::DEALER)
        @client.setsockopt ZMQ::LINGER, 0
        @client.setsockopt ZMQ::IDENTITY, config[:id]
        @client.connect(config[:server])
      end

      def ask?
      end

      def tell(target, message)
        ZMessage.new("", UUIDTools::UUID.random_create.to_s, "fireforget", "", target, message).send_to @client
      end

    end

  end
end
