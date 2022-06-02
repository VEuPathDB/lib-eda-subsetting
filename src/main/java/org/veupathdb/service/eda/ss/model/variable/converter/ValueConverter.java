package org.veupathdb.service.eda.ss.model.variable.converter;

/**
 * Base interface for serializers
 *
 * @param <T> type of intermediate value directly translatable to byte[]
 */
public interface ValueConverter<T> {

  T fromString(String s);

  byte[] toBytes(T varValue);

  T fromBytes(byte[] bytes);

  int numBytes();

}