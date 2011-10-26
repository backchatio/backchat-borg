# coding: utf-8
# -*- encoding: utf-8 -*-

module Backchat
  module Borg

    class Client

      attr_accessor :id

      def initialize(config)
        @id = config[:id]
        @client = Backchat::Borg.context.socket(ZMQ::DEALER)
        @client.setsockopt ZMQ::LINGER, 0
        @client.setsockopt ZMQ::IDENTITY, config[:id]
        @client.connect config[:server]

      end

    #   
    # poller.poll(receiveTimeout.millis * 1000)
    # if (poller.pollin(0)) {
    #   val msg = ZMessage(client)
    #   if (msg.messageType == "system" && msg.sender == "ERROR") {
    #     msg.body match {
    #       case "SERVER_UNAVAILABLE" ⇒ {
    #         throw new ServerUnavailableException
    #       }
    #       case "TIMEOUT" ⇒ {
    #         throw new RequestTimeoutException("The request to " + target + " with data: " + appEvent.toJson + " timed out.")
    #       }
    #     }
    #   } else {
    #     onReply(ApplicationEvent(msg.body))
    #   }
    # } else {
    #   throw new RequestTimeoutException("The request to " + target + " with data: " + appEvent.toJson + " timed out.")
    # }

      def new_ccid
        UUIDTools::UUID.random_create.to_s
      end

      def ask(target, app_event) # request reply
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", new_ccid, "requestreply", id, target, message).send_to client
      end

      def tell(target, app_event)
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", new_ccid, "fireforget", "", target, message).send_to @client
      end

      def listen(topic)
        # listen to a pubsub topic  
      end

      def shout(topic)
        # publish to pubsub topic
      end

      def disconnect 
        @client.close
      end

    end

  end
end
