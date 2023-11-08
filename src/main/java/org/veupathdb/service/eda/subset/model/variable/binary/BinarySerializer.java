package org.veupathdb.service.eda.subset.model.variable.binary;


/**
 * Base interface for converting values of type T to binary.
 *
 * @param <T> Type of value to convert to binary.
 */

public interface BinarySerializer<T> {
  byte[] toBytes(T varValue);

  int numBytes();
}
