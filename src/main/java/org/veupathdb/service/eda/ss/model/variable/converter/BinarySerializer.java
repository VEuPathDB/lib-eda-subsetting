package org.veupathdb.service.eda.ss.model.variable.converter;

import java.nio.ByteBuffer;

public interface BinarySerializer<T> {
  byte[] toBytes(T varValue);

  T fromBytes(byte[] bytes);

  int numBytes();
}
