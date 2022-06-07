package org.veupathdb.service.eda.ss.model.variable.converter;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdSerializer<T> {
  private static final int ID_BYTE_COUNT = 20;

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
  public byte[] convertToBytes(VariableValueIdPair<T> variable) {
    final int bufferSize = totalBytesNeeded();
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
  public VariableValueIdPair<T> convertFromBytes(byte[] bytes) {
    final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    final byte[] varValueBytes = new byte[_valueConverter.numBytes()];
    final Long index = byteBuffer.getLong();
    byteBuffer.get(varValueBytes);
    final T varValue = _valueConverter.fromBytes(varValueBytes);
    return new VariableValueIdPair<>(index, varValue);
  }

  public int totalBytesNeeded() {
    return ID_BYTE_COUNT + _valueConverter.numBytes();
  }
}

