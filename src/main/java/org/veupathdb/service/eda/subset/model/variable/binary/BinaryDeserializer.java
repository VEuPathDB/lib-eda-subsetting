package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;

/**
 * Base interface for converting binary to a value of type T.
 *
 * @param <T> Type of value to convert to from binary
 */
public interface BinaryDeserializer<T> {
  T fromBytes(byte[] bytes);

  T fromBytes(byte[] bytes, int offset);

  T fromBytes(ByteBuffer buffer);

  int numBytes();
}
