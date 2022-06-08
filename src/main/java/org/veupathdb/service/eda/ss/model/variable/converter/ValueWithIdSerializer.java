package org.veupathdb.service.eda.ss.model.variable.converter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdSerializer<T> implements BinarySerializer<VariableValueIdPair<T>> {

  private final ValueConverter<T> _valueConverter;

  public ValueWithIdSerializer(ValueConverter<T> valueConverter) {
    _valueConverter = valueConverter;
  }

  /**
   * Converts a variable to an array of bytes. The first 4 bytes are written as an integer variable identifier and the
   * next N are read as the variable value.
   * @param variable to convert to bytes
   * @return Deserialized variable object
   */
  @Override
  public byte[] toBytes(VariableValueIdPair<T> variable) {
    final int bufferSize = numBytes();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putLong(variable.getIndex());
    byteBuffer.put(_valueConverter.toBytes(variable.getValue()));
    return byteBuffer.array();
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
