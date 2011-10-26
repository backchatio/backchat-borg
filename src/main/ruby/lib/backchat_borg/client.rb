# coding: utf-8
# -*- encoding: utf-8 -*-

module Backchat
  module Borg

    class Client

      attr_accessor :id, :receive_timeout

      def initialize(config)
        @id, @receive_timeout = config[:id], (config[:receive_timeout]||3.seconds)
        @client = Backchat::Borg.context.socket(ZMQ::DEALER)
        @client.setsockopt ZMQ::LINGER, 0
        @client.setsockopt ZMQ::IDENTITY, config[:id]
        @client.connect config[:server]
        @poller = ZMQ::Poller.new
        @poller.register_readable @client
      end

      def new_ccid
        UUIDTools::UUID.random_create.to_s
      end

      def tell(target, app_event) 
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", new_ccid, "fireforget", "", target, message).send_to @client
      end

      def ask(target, app_event, &on_reply) # request reply
        raise "on_reply needs to be provided as a block to handle the reply of the message" if on_reply.nil?
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", new_ccid, "requestreply", id, target, message).send_to @client
        rc = @poller.poll(receive_timeout * 1000)
        if rc > 0
          handle_reply target, message, &on_reply
        else
          raise RequestTimeoutException, "The request to #{target} with data: #{message} timed out."
        end
      end

      def listen(topic)
        # listen to a pubsub topic  
      end

      def shout(topic)
        # publish to pubsub topic
      end

      def disconnect 
        @poller.deregister_readable @client
        @client.close
      end

      private
      def raise_if_error_reply(msg)
        if msg.message_type == "system" && msg.sender == "ERROR"
          case msg.body
          when "SERVER_UNAVAILABLE"
            raise ServerUnavailableException
          when "TIMEOUT"
            raise RequestTimeoutException, "The request to #{target} with data: #{message} timed out."
          end
        end
      end

      def handle_reply(target, message, &on_reply)
        msg = ZMessage.read(@client)
        raise_if_error_reply msg
        on_reply.call ActiveSupport::JSON.decode(msg.body)
      end

    end

  end
end
