package org.veupathdb.service.eda.ss.model.variable.converter;


public interface BinarySerializer<T> {
  byte[] toBytes(T varValue);

  T fromBytes(byte[] bytes);

  int numBytes();
}
