package com.mojolly.backchat.zeromq.tests

import org.scalatest.{ BeforeAndAfterAll, GivenWhenThen, FeatureSpec }
import com.mojolly.backchat.zeromq.ZeroMQ

class ServiceDiscoveryIntegrationSpec extends FeatureSpec with GivenWhenThen with BeforeAndAfterAll {

  feature("service discovery") {

    info("As a client")
    info("I want to discover the capabilities of the system")
    info("so that I know where to go to get answers to my questions")

    scenario("a service provides pubsub") {
      given("a started backend service with pubsub")
      when("a client asks for the capabilities")
      then("the server should include pubsub in its response")
      pending
    }

    scenario("a service provides load-balanced push") {
      given("a started frontend service with push")
      when("a client says it's ready")
      then("the server should ask for the client capabilities")
      and("the client should include push in its response")
      pending
    }

    scenario("a service provides an api service") {
      given("a service that provides authentication")
      when("the client asks for a service that provides authentication")
      then("the server should response with the service name of the provider")
      pending
    }
  }
}