require File.dirname(__FILE__) + "/spec_helper"

describe Backchat::Borg::ZMessage do
  ccid = UUIDTools::UUID.random_create.to_s
  zmsg = ZMessage.new("sender-1", "sender-2", "", ccid, "requestreply", "the-sender", "the-target", "the body")


  it "return the size of its parts" do
    msg = ZMessage.new("hello", "world", "the", "message", "is", "this")
    msg.size.should eql(6)
  end

  it "get the correct part as body" do
    zmsg.body.should eql("the body")
  end

  it "get the message type" do
    zmsg.message_type.should eql("requestreply")
  end

  it "get the ccid" do
    zmsg.ccid.should eql(ccid)
  end

  it "get the sender" do
    zmsg.sender.should eql("the-sender")
  end

  it "get the target" do
    zmsg.target.should eql("the-target")
  end

  it "get the socket addresses along the way" do
    zmsg.addresses.should eql(["sender-1", "sender-2"])
  end

  it "allow replacing the addresses" do
    zmsg.addresses = %w(new-sender-1 new-sender-2)
    zmsg.addresses.should eql(["new-sender-1", "new-sender-2"])
  end
end