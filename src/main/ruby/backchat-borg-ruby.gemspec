# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "backchat_client/version"

Gem::Specification.new do |s|
  s.name        = "backchat-borg-ruby"
  s.version     = Backchat::Borg::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["This gem provides an easy way for writing external extensions to the backchat platform"]
  s.email       = ["ivan@mojolly.com"]
  s.homepage    = ""
  s.summary     = %q{Gem to extend the backchat platform}
  s.description = %q{Gem to extend the backchat platform}

  s.rubyforge_project = "backchat-borg-ruby"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_dependency("activesupport")
  s.add_dependency("addressable")  
  s.add_dependency("zmq")
  #s.add_dependency("zookeeper")
  s.add_development_dependency('rspec')
end