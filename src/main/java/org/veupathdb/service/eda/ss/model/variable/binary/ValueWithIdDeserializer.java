package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdDeserializer<T> implements BinaryDeserializer<VariableValueIdPair<T>> {
  private static final long BIT_MASK = 0xFF;

  private final BinaryDeserializer<T> _valueConverter;
  private final VariableValueIdPair<T> _holder = new VariableValueIdPair<>(-1L, null);

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
    return fromBytes(bytes, 0);
  }

  @Override
  public VariableValueIdPair<T> fromBytes(byte[] bytes, int offset) {
    long idIndex = 0L;
    for (int i = offset; i < offset + 8; i++) {
      idIndex <<= 8;
      idIndex |= (bytes[i] & BIT_MASK);
    }
    T value = _valueConverter.fromBytes(bytes, offset + 8);
    _holder.setIdIndex(idIndex);
    _holder.setValue(value);
    return _holder;
  }

  @Override
  public VariableValueIdPair<T> fromBytes(ByteBuffer buffer) {
    final long idIndex = buffer.getLong();
    final T varValue = _valueConverter.fromBytes(buffer);
    _holder.setValue(varValue);
    _holder.setIdIndex(idIndex);
    return _holder;
  }

  @Override
  public int numBytes() {
    return Long.BYTES + _valueConverter.numBytes();
  }
}

