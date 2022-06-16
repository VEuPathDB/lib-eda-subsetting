package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdSerializer<T> implements BinarySerializer<VariableValueIdPair<T>> {

  private final BinaryConverter<T> _valueConverter;

  public ValueWithIdSerializer(BinaryConverter<T> valueConverter) {
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

  @Override
  public int numBytes() {
    return Long.BYTES + _valueConverter.numBytes();
  }
}

