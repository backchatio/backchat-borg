package com.mojolly.backchat.zeromq.tests

import org.scalatest.Suites

class ZeroMqSuite extends Suites(
  new ZMessageSpec,
  new ServiceRegistrySpec,
  new ServiceRegistryBridgeSpec,
  new ProtocolMessageSpec,
  new ClientBrokerSpec,
  new ReliableClientBrokerSpec,
  new BackchatZeroMqClientSpec,
  new ServerActorBridgeSpec,
  new ZeroMqBridgeSpec,
  new ZeroMqClientActorBridgeSpec,
  new ZeroMqIntegrationSpec)