package com.retailsvc.gcp.pubsub;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A general-purpose mapper for objects to byte representation.
 *
 * @author sasjo
 */
@FunctionalInterface
public interface ObjectToBytesMapper {

  /**
   * Convert a value to bytes.
   *
   * @param value a value
   * @return the byte representation of the value.
   * @throws IOException if failing to convert to bytes.
   */
  ByteBuffer valueAsBytes(Object value) throws IOException;
}
