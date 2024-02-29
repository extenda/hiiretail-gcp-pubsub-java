package com.retailsvc.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;

/**
 * Factory to create a Pub/Sub publisher for a topic.
 *
 * @author sasjo
 */
@FunctionalInterface
public interface PublisherFactory {

  /**
   * Create a new publisher.
   *
   * @param topic the topic name
   * @return the created publisher.
   */
  Publisher.Builder newBuilder(TopicName topic);
}
