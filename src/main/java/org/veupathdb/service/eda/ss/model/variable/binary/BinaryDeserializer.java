package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;

/**
 * Base interface for converting binary to a value of type {@param T}.
 *
 * @param <T> Type of value to convert to from binary
 */
public interface BinaryDeserializer<T> {
  T fromBytes(byte[] bytes);

  T fromBytes(ByteBuffer buffer);

  int numBytes();
}
