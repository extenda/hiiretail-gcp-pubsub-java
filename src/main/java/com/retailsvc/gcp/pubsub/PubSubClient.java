package com.retailsvc.gcp.pubsub;

import java.util.Map;

/**
 * A client to send messages to Google Cloud PubSub.
 *
 * <p>Instantiate a {@link PubSubClient} via {@link PubSubClientFactory}.
 */
public interface PubSubClient extends AutoCloseable {

  /**
   * Publish a message on PubSub. The method will block until either a successful publishing
   * completes, or an error is raised.
   *
   * @param payloadObject The payload to send
   * @param attributesMap The map of attributes to send
   * @throws PubSubClientException If an error occurs, or the message cannot be delivered
   */
  void publish(Object payloadObject, Map<String, String> attributesMap)
      throws PubSubClientException;

  /**
   * Publish a message on PubSub using a key for ordering. The method will block until either a
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
   * @return True if client has been closed, false otherwise.
   */
  boolean isClosed();

  @Override
  void close();
}
