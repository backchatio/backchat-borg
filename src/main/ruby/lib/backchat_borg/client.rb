# coding: utf-8
# -*- encoding: utf-8 -*-

module Backchat
  module Borg

    #
    # A ZeroMQ based client for the backchat servers
    # This client represents a connection to a server to make requests
    # and get replies.
    # 
    class Client

      attr_accessor :id, :receive_timeout

      #
      # Initialize this connection
      # @param Hash config with the possible keys:
      #   * :id  The id of this client
      #   * :receive_timeout The timeout for a request in seconds
      #   * :server The zeromq connection string for the server address
      #
      def initialize(config)
        @id, @receive_timeout = config[:id], (config[:receive_timeout]||3.seconds)
        raise "A client needs an :id" if @id.nil?
        raise "A client needs a :server to connect to." if config[:server].nil?
        @client = Backchat::Borg.context.socket(ZMQ::DEALER)
        @client.setsockopt ZMQ::LINGER, 0
        @client.setsockopt ZMQ::IDENTITY, config[:id]
        @client.connect config[:server]
        @poller = ZMQ::Poller.new
        @poller.register_readable @client
      end


      # 
      # Enqueues an event for the specified target with the specified payload
      #
      # @param target String The target for the event
      # @param app_event Object The payload for the event
      #
      def tell(target, app_event) 
        assert_params "tell", target, app_event
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", Backchat::Borg.new_ccid, "fireforget", "", target, message).send_to @client
      end


      # 
      # Makes a request that expects a reply.
      # If there is no reply within the configured request timeout an error is raised.
      # When the server is no longer available an error is raised.
      #
      # @param target String The target for the event
      # @param app_event Object The payload for the event
      # @param on_reply Lambda The callback to process the reply
      #
      def ask(target, app_event, &on_reply) # request reply
        assert_params "ask", target, app_event
        #raise "on_reply needs to be provided as a block for ask to handle the reply of the message" if on_reply.nil?
        message = app_event.is_a?(String) ? app_event : app_event.to_json
        ZMessage.new("", Backchat::Borg.new_ccid, "requestreply", id, target, message).send_to @client
        rc = @poller.poll(receive_timeout * 1000)
        if rc > 0
          handle_reply target, message, &on_reply
        else
          raise RequestTimeoutException, "The request to #{target} with data: #{message} timed out."
        end
      end

      # 
      # Disconnect this client
      #
      def disconnect 
        @poller.deregister_readable @client
        @client.close
      end

      private
      def raise_if_error_reply(msg) #:nodoc:
        if msg.message_type == "system" && msg.sender == "ERROR"
          case msg.body
          when "SERVER_UNAVAILABLE"
            raise ServerUnavailableException
          when "TIMEOUT"
            raise RequestTimeoutException, "The request to #{target} with data: #{message} timed out."
          end
        end
      end

      def handle_reply(target, message, &on_reply) #:nodoc:
        msg = ZMessage.read(@client)
        raise_if_error_reply msg
        decoded = ActiveSupport::JSON.decode(msg.body)
        on_reply.nil? ? decoded : on_reply.call(decoded)
      end

      def assert_params(name, target, app_event)
        raise ArgumentError, "A target is required for #{name}" if target.nil?
        raise ArgumentError, "Event data is required for #{name}" if app_event.nil?
      end

    end

  end
end
