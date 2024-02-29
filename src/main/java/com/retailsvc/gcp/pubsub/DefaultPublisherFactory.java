package com.retailsvc.gcp.pubsub;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.TopicName;

/**
 * A default publisher factory that uses the PubSub default settings.
 *
 * @author sasjo
 */
public class DefaultPublisherFactory implements PublisherFactory {

  @Override
  public Publisher.Builder newBuilder(TopicName topic) {
    return Publisher.newBuilder(topic);
  }
}
