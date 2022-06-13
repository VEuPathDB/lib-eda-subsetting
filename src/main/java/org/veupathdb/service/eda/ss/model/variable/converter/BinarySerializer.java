package org.veupathdb.service.eda.ss.model.variable.converter;


public interface BinarySerializer<T> extends BinaryDeserializer<T> {
  byte[] toBytes(T varValue);

  int numBytes();
}
