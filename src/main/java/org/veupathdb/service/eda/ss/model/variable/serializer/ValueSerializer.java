package org.veupathdb.service.eda.ss.model.variable.serializer;

/**
 * Base interface for serializers
 *
 * @param <T> type of intermediate value directly translatable to byte[]
 */
public interface ValueSerializer<T> {

  byte[] toBytes(T varValue);

  T fromBytes(byte[] bytes);

  int numBytes();

}