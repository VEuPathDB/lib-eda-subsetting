package org.veupathdb.service.eda.ss.model.variable.converter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdDeserializer<T> implements BinaryDeserializer<VariableValueIdPair<T>> {

  private final BinaryDeserializer<T> _valueConverter;

  public ValueWithIdDeserializer(BinaryDeserializer<T> valueConverter) {
    _valueConverter = valueConverter;
  }

  /**
   * Converts an array of bytes to a variable. The first 4 bytes are read as an integer and the next N are read as
   * the variable value.
   * @param bytes to convert to variable
   * @return Deserialized variable object
   */
  @Override
  public VariableValueIdPair<T> fromBytes(byte[] bytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final byte[] varValueBytes = new byte[_valueConverter.numBytes()];
    final Long index = byteBuffer.getLong();
    byteBuffer.get(varValueBytes);
    final T varValue = _valueConverter.fromBytes(varValueBytes);
    return new VariableValueIdPair<>(index, varValue);
  }

  @Override
  public int numBytes() {
    return Long.BYTES + _valueConverter.numBytes();
  }
}

