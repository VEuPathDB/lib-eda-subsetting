package org.veupathdb.service.eda.subset.model.variable.binary;

import org.veupathdb.service.eda.subset.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdSerializer<V> implements BinarySerializer<VariableValueIdPair<V>> {

  private final BinarySerializer<V> _valueSerializer;

  public ValueWithIdSerializer(BinarySerializer<V> valueSerializer) {
    _valueSerializer = valueSerializer;
  }

  /**
   * Converts a variable to an array of bytes. The first 4 bytes are written as an integer variable identifier and the
   * next N are read as the variable value.
   * @param variable to convert to bytes
   * @return Deserialized variable object
   */
  @Override
  public byte[] toBytes(VariableValueIdPair<V> variable) {
    final int bufferSize = numBytes();
    final ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
    byteBuffer.putLong(variable.getIdIndex());
    byteBuffer.put(_valueSerializer.toBytes(variable.getValue()));
    return byteBuffer.array();
  }

  @Override
  public int numBytes() {
    return Long.BYTES + _valueSerializer.numBytes();
  }
}

