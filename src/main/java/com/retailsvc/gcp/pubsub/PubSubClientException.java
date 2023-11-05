package com.retailsvc.gcp.pubsub;

public class PubSubClientException extends RuntimeException {

  public PubSubClientException(String message) {
    super(message);
  }

  public PubSubClientException(String message, Throwable cause) {
    super(message, cause);
  }
}
