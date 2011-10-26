# coding: utf-8

module Backchat
  module Borg
    class StandardError < ::StandardError; end
    class ServerUnavailableException < StandardError; end
    class RequestTimeoutException < StandardError; end
  end
end

# vim: set si ts=2 sw=2 sts=2 et:
