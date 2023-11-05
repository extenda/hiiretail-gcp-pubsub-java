package com.retailsvc.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubClientFactoryTest {

  PubSubClientFactory factory;

  @BeforeEach
  void setUp() {
    factory = new PubSubClientFactory();
  }

  @Test
  void testClientsAreCached() {
    PubSubClient client1 = factory.create("test");
    PubSubClient client2 = factory.create("test");

    assertThat(client1).isSameAs(client2);
  }
}
