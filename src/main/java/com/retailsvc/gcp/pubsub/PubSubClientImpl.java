package com.retailsvc.gcp.pubsub;

import static java.util.Objects.nonNull;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private final ObjectToBytesMapper objectMapper;
  private final int publishTimeout;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  public PubSubClientImpl(Supplier<Publisher> publisherFactory, ObjectToBytesMapper objectMapper) {
    Objects.requireNonNull(publisherFactory);
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
    publishOrdered(payloadObject, attributesMap, null);
  }

  @Override
  public void publishOrdered(
      Object payloadObject, Map<String, String> attributesMap, String orderingKey)
      throws PubSubClientException {
    if (isClosed()) {
      throw new PubSubClientException("Client is closed");
    }

    var attributes = Optional.ofNullable(attributesMap).orElseGet(HashMap::new);
    publish(toByteString(payloadObject), attributes, orderingKey);
  }

  @Override
  public List<String> publishAll(List<OutgoingMessage> messages) throws PubSubClientException {
    if (isClosed()) {
      throw new PubSubClientException("Client is closed");
    }
    Objects.requireNonNull(messages, "messages must not be null");
    if (messages.isEmpty()) {
      return List.of();
    }

    // Build every message up front so a malformed payload fails fast, before anything is sent.
    var pubsubMessages = messages.stream().map(this::toPubsubMessage).toList();

    // Submit all messages before awaiting any result, so the underlying publisher can batch them
    // together. Waiting per message (as publish does) keeps only one message in flight and defeats
    // batching.
    var futures = pubsubMessages.stream().map(publisher::publish).toList();

    return awaitAll(futures);
  }

  private PubsubMessage toPubsubMessage(OutgoingMessage message) {
    var attributes = Optional.ofNullable(message.attributes()).orElseGet(HashMap::new);
    var builder =
        PubsubMessage.newBuilder()
            .putAllAttributes(attributes)
            .setData(toByteString(message.payload()));
    if (nonNull(message.orderingKey())) {
      builder.setOrderingKey(message.orderingKey());
    }
    return builder.build();
  }

  private List<String> awaitAll(List<ApiFuture<String>> futures) {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(publishTimeout);
    var ids = new ArrayList<String>(futures.size());
    var failures = 0;
    PubSubClientException firstFailure = null;
    for (var future : futures) {
      try {
        var remaining = deadline - System.nanoTime();
        ids.add(future.get(Math.max(remaining, 0), TimeUnit.NANOSECONDS));
      } catch (ExecutionException e) {
        failures++;
        firstFailure =
            Optional.ofNullable(firstFailure)
                .orElseGet(() -> new PubSubClientException("Generic execution error", e));
      } catch (TimeoutException e) {
        failures++;
        firstFailure =
            Optional.ofNullable(firstFailure)
                .orElseGet(
                    () -> new PubSubClientException("Timed out waiting for publish result", e));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PubSubClientException("Interrupted while waiting for publish result", e);
      }
    }
    if (failures > 0) {
      throw new PubSubClientException(
          "Failed to publish %d of %d messages".formatted(failures, futures.size()), firstFailure);
    }
    return ids;
  }

  private ByteString toByteString(Object payloadObject) {
    try {
      return switch (payloadObject) {
        case String s -> ByteString.copyFromUtf8(s);
        case ByteBuffer b -> ByteString.copyFrom(b);
        case InputStream i -> ByteString.readFrom(i);
        case null -> throw new PubSubClientException("Payload object cannot be null");
        default -> ByteString.copyFrom(mapValue(payloadObject));
      };
    } catch (NullPointerException | IOException e) {
      throw new PubSubClientException("Could not read payload", e);
    }
  }

  private ByteBuffer mapValue(Object payloadObject) throws IOException {
    if (objectMapper == null) {
      throw new IOException("No object mapper configured");
    }
    return objectMapper.valueAsBytes(payloadObject);
  }

  @Override
  public boolean isClosed() {
    return isClosed.get();
  }

  protected void publish(ByteString payload, Map<String, String> attributes, String orderingKey) {
    var pubsubMessage = PubsubMessage.newBuilder().putAllAttributes(attributes).setData(payload);
    if (nonNull(orderingKey)) {
      pubsubMessage.setOrderingKey(orderingKey);
    }

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
