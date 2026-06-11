package com.retailsvc.gcp.pubsub;

import java.util.Map;

/**
 * A single message to publish as part of a batch via {@link
 * PubSubClient#publishAll(java.util.List)}.
 *
 * @param payload the message payload, mapped to bytes the same way as {@link
 *     PubSubClient#publish(Object, Map)}
 * @param attributes the message attributes, may be {@code null}
 * @param orderingKey the ordering key, or {@code null} to publish without ordering
 */
public record OutgoingMessage(Object payload, Map<String, String> attributes, String orderingKey) {

  /**
   * Create an unordered message.
   *
   * @param payload the message payload
   * @param attributes the message attributes, may be {@code null}
   * @return a new message without an ordering key
   */
  public static OutgoingMessage of(Object payload, Map<String, String> attributes) {
    return new OutgoingMessage(payload, attributes, null);
  }

  /**
   * Create a message published with an ordering key.
   *
   * @param payload the message payload
   * @param attributes the message attributes, may be {@code null}
   * @param orderingKey the ordering key
   * @return a new message with the given ordering key
   */
  public static OutgoingMessage ordered(
      Object payload, Map<String, String> attributes, String orderingKey) {
    return new OutgoingMessage(payload, attributes, orderingKey);
  }
}
