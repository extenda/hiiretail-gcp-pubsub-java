package com.retailsvc.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
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

    try {
      ByteString payload =
          switch (payloadObject) {
            case String s -> ByteString.copyFromUtf8(s);
            case ByteBuffer b -> ByteString.copyFrom(b);
            case InputStream i -> ByteString.readFrom(i);
            case null -> throw new PubSubClientException("Payload object cannot be null");
            default -> ByteString.copyFrom(mapValue(payloadObject));
          };
      var attributes = Optional.ofNullable(attributesMap).orElseGet(HashMap::new);
      publish(payload, attributes, orderingKey);
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
    if (orderingKey != null) {
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
