package com.retailsvc.gcp.pubsub;

/**
 * Configuration class for the PubSub client, allowing customization such as enabling message
 * ordering.
 */
public class PubSubClientConfig {
  private boolean messageOrderingEnabled;

  public boolean isMessageOrderingEnabled() {
    return messageOrderingEnabled;
  }

  public PubSubClientConfig setMessageOrderingEnabled(boolean messageOrderingEnabled) {
    this.messageOrderingEnabled = messageOrderingEnabled;
    return this;
  }
}
