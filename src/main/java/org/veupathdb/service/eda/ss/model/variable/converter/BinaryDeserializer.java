package org.veupathdb.service.eda.ss.model.variable.converter;

public interface BinaryDeserializer<T> {
  T fromBytes(byte[] bytes);

  int numBytes();
}
