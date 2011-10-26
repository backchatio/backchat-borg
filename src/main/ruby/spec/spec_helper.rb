require 'backchat_borg'

Backchat::Borg.logger = Logger.new("/dev/null")


RSpec.configure do |c|

end

shared_context "zeromq_context" do
  before(:all) do
    @zmq = Backchat::Borg.context
  end

  after(:all) do
    Backchat::Borg.context = nil
  end

end

