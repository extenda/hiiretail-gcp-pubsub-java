package com.retailsvc.gcp.pubsub;

import static com.retailsvc.gcp.pubsub.EmulatorRedirect.PUBSUB_EMULATOR_HOST;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String TEST_PROJECT = "test-project";
  public static final String PROJECT_ID = "SERVICE_PROJECT_ID";

  private final Map<String, PubSubClient> clientCache = new ConcurrentHashMap<>();
  private final ObjectToBytesMapper objectMapper;
  private final PublisherFactory publisherFactory;
  private final ReentrantLock lock = new ReentrantLock();

  public PubSubClientFactory() {
    this((ObjectToBytesMapper) null);
  }

  public PubSubClientFactory(ObjectToBytesMapper objectMapper) {
    this(objectMapper, new DefaultPublisherFactory());
  }

  public PubSubClientFactory(PublisherFactory publisherFactory) {
    this(null, publisherFactory);
  }

  public PubSubClientFactory(ObjectToBytesMapper objectMapper, PublisherFactory publisherFactory) {
    this.objectMapper = objectMapper;
    this.publisherFactory = publisherFactory;
  }

  /**
   * Create a new client for publishing messages onto PubSub. Once created, the client will be
   * cached internally and subsequent calls for the same topic will return the same instance.
   *
   * @param topic The topic for the client.
   * @return The PubSub client.
   */
  public PubSubClient create(String topic) {
    try {
      lock.lock();

      Optional.ofNullable(clientCache.get(topic))
          .filter(PubSubClient::isClosed)
          .ifPresent(ignored -> clientCache.remove(topic));
      return clientCache.computeIfAbsent(topic, this::newClient);
    } finally {
      lock.unlock();
    }
  }

  private PubSubClient newClient(String topic) {
    LOG.debug("Creating a new client [{}]", topic);
    return new PubSubClientImpl(publisherFactory(topic), objectMapper);
  }

  private Supplier<Publisher> publisherFactory(String topic) {
    return () -> {
      try {
        var builder = publisherFactory.newBuilder(createTopic(topic));
        emulatorHost().ifPresent(ignored -> EmulatorRedirect.redirect(builder));
        return builder.build();
      } catch (IOException e) {
        throw new PubSubClientException("Cant create Pubsub client", e);
      }
    };
  }

  private static TopicName createTopic(String topic) {
    var projectId = Objects.requireNonNullElse(System.getenv(PROJECT_ID), TEST_PROJECT);
    return TopicName.of(projectId, topic);
  }

  private Optional<String> emulatorHost() {
    return Optional.ofNullable(System.getProperty(PUBSUB_EMULATOR_HOST))
        .or(() -> Optional.ofNullable(System.getenv(PUBSUB_EMULATOR_HOST)));
  }
}
