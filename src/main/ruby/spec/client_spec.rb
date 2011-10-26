# coding: utf-8
require File.dirname(__FILE__) + "/spec_helper"

describe Backchat::Borg::Client do
  include_context "zeromq_context"

  it "can enqueue a message to the server" do
    pending
  end

  it "can request a message with a reply" do
    pending
  end

  it "throws a RequestTimeoutException when the backend responds with that" do
    pending 
  end
end

# vim: set si ts=2 sw=2 sts=2 et:
