package org.veupathdb.service.eda.ss.model.variable.converter;


/**
 * Base interface for converting values of type {@param T} to binary.
 *
 * @param <T> Type of value to convert to binary.
 */

public interface BinarySerializer<T> {
  byte[] toBytes(T varValue);

  int numBytes();
}
