package com.retailsvc.gcp.pubsub;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A client to send messages to Google Cloud PubSub.
 *
 * <p>Instantiate a {@link PubSubClient} via {@link PubSubClientFactory}.
 */
public interface PubSubClient extends AutoCloseable {

  /**
   * Publish a message on PubSub. The method will block until either successful publishing
   * completes, or an error is raised.
   *
   * @param payloadObject The payload to send
   * @param attributesMap The map of attributes to send
   * @throws PubSubClientException If an error occurs, or the message cannot be delivered
   */
  void publish(Object payloadObject, Map<String, String> attributesMap)
      throws PubSubClientException;

  /**
   * Publish a message on PubSub using a key for ordering. The method will block until either
   * successful publishing completes, or an error is raised.
   *
   * @param payloadObject The payload to send
   * @param attributesMap The map of attributes to send
   * @param orderingKey The key used for ordering of the messages
   * @throws PubSubClientException If an error occurs, or the message cannot be delivered
   */
  void publishOrdered(Object payloadObject, Map<String, String> attributesMap, String orderingKey)
      throws PubSubClientException;

  /**
   * Publish a batch of messages on PubSub. All messages are handed to the underlying publisher
   * first, allowing it to batch them into as few publish requests as possible, and only then are
   * the results awaited. This is more efficient than calling {@link #publish(Object, Map)} per
   * message, which blocks on each result and prevents batching.
   *
   * <p>Every message is always submitted, even if some fail. If one or more messages fail to
   * publish, a {@link PubSubClientException} is thrown after all results have been awaited, with
   * the first failure as its cause.
   *
   * @param messages the messages to publish, in the order their ids are returned
   * @return the published message ids, in the same order as {@code messages}
   * @throws PubSubClientException if the client is closed or any message fails to publish
   */
  List<String> publishAll(List<OutgoingMessage> messages) throws PubSubClientException;

  /**
   * Publish a batch of payloads sharing the same attributes. Convenience wrapper around {@link
   * #publishAll(List)}.
   *
   * @param payloads the payloads to publish
   * @param attributes the attributes applied to every message, may be {@code null}
   * @return the published message ids, in the same order as {@code payloads}
   * @throws PubSubClientException if the client is closed or any message fails to publish
   */
  default List<String> publishAll(Collection<?> payloads, Map<String, String> attributes)
      throws PubSubClientException {
    return publishAll(
        payloads.stream().map(payload -> OutgoingMessage.of(payload, attributes)).toList());
  }

  /**
   * @return True if the client has been closed, false otherwise.
   */
  boolean isClosed();

  @Override
  void close();
}
