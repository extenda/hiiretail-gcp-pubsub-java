package com.retailsvc.gcp.pubsub;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PubSubClientImpl implements PubSubClient {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PUBSUB_CLOSE_TIMEOUT_SECONDS = "PUBSUB_CLOSE_TIMEOUT_SECONDS";
  private static final int DEFAULT_CLOSE_TIMEOUT = 10;
  private static final String PUBSUB_WAIT_PUBLISH_SECONDS = "PUBSUB_WAIT_PUBLISH_SECONDS";
  private static final int PUBLISH_TIMEOUT = 30;

  private final Publisher publisher;
  private final ObjectMapper objectMapper;
  private final int publishTimeout;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public PubSubClientImpl(Supplier<Publisher> publisherFactory, ObjectMapper objectMapper) {
    Objects.requireNonNull(publisherFactory);
    Objects.requireNonNull(objectMapper, "You need to supply an instance of ObjectMapper");
    this.objectMapper = objectMapper;
    this.publisher = publisherFactory.get();
    Objects.requireNonNull(this.publisher);

    this.publishTimeout =
        Optional.ofNullable(System.getenv(PUBSUB_WAIT_PUBLISH_SECONDS))
            .map(Integer::parseInt)
            .orElse(PUBLISH_TIMEOUT);
  }

  @Override
  public void publish(Object payloadObject, Map<String, String> attributesMap) {
    if (isClosed()) {
      throw new PubSubClientException("Client is closed");
    }

    var attributes = Optional.ofNullable(attributesMap).orElseGet(HashMap::new);

    try {
      if (payloadObject instanceof String stringPayload) {
        publish(ByteString.copyFromUtf8(stringPayload), attributes);
      } else {
        var payloadBytes = objectMapper.writeValueAsBytes(payloadObject);
        publish(ByteString.copyFrom(payloadBytes), attributes);
      }
    } catch (NullPointerException | JsonProcessingException e) {
      throw new PubSubClientException("Could not read payload", e);
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  protected void publish(ByteString payload, Map<String, String> attributes) {
    var pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(attributes).setData(payload);

    try {
      ApiFuture<String> publishResult = publisher.publish(pubsubMessage.build());
      String id = publishResult.get(publishTimeout, TimeUnit.SECONDS);
      LOG.debug("Message [{}] published", id);
    } catch (ExecutionException e) {
      throw new PubSubClientException("Generic execution error", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException("Interrupted while waiting for publish result", e);
    } catch (TimeoutException e) {
      throw new PubSubClientException("Timed out waiting for publish result", e);
    }
  }

  @Override
  public void close() {
    try {
      Integer timeout =
          Optional.ofNullable(System.getenv(PUBSUB_CLOSE_TIMEOUT_SECONDS))
              .map(Integer::parseInt)
              .orElse(DEFAULT_CLOSE_TIMEOUT);
      this.isClosed.set(true);
      publisher.shutdown();
      publisher.awaitTermination(timeout, TimeUnit.SECONDS);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      LOG.error("Interrupted while closing client");
    }
  }
}
