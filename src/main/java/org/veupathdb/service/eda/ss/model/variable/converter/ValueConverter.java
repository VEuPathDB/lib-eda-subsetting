package org.veupathdb.service.eda.ss.model.variable.converter;

/**
 * Base interface for serializers
 *
 * @param <T> type of intermediate value directly translatable to byte[]
 */
public interface ValueConverter<T> extends BinarySerializer<T> {

  T fromString(String s);

}