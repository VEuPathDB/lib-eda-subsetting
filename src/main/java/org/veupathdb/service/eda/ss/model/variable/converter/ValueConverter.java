package org.veupathdb.service.eda.ss.model.variable.converter;

/**
 * Interface encapsulating all binary serialization. Also encapsulates string serialization which for interfacing with
 * database ResultSets.
 *
 * @param <T> type of intermediate value directly translatable to byte[]
 */
public interface ValueConverter<T> extends BinaryConverter<T> {

  /**
   * FIXME:
   * This method is more closely related to logic in the {@link org.veupathdb.service.eda.ss.model.variable.VariableType}
   * class as opposed to binary serialization.
   */
  T fromString(String s);

}