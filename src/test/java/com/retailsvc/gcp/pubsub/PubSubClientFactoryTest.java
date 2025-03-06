package com.retailsvc.gcp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubClientFactoryTest {

  PubSubClientFactory factory;

  @BeforeEach
  void setUp() {
    System.setProperty(EmulatorRedirect.PUBSUB_EMULATOR_HOST, "localhost:8085");
    factory = new PubSubClientFactory();
  }

  @Test
  void testClientsAreCached() {
    PubSubClient client1 = factory.create("test");
    PubSubClient client2 = factory.create("test");

    assertThat(client1).isSameAs(client2);
  }

  @Test
  void customMapper() {
    ObjectToBytesMapper mapper = mock();
    var custom = assertDoesNotThrow(() -> new PubSubClientFactory(mapper));
    try (var client = assertDoesNotThrow(() -> custom.create("test"))) {
      assertNotNull(client);
    }
  }

  @Test
  void pooledFactory() {
    var pooled = new PubSubClientFactory(PooledPublisherFactory.defaultPool());
    try (var client = assertDoesNotThrow(() -> pooled.create("test"))) {
      assertNotNull(client);
    }
  }

  @Test
  void thatConfigCanBeSet() {
    final var clientFactory = new PubSubClientFactory().setClientConfig(new PubSubClientConfig());
    try (var client = clientFactory.create("test")) {
      assertNotNull(client);
    }
  }
}
