package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.nio.ByteBuffer;

public class ValueWithIdDeserializer<T> implements BinaryDeserializer<VariableValueIdPair<T>> {

  private final BinaryDeserializer<T> _valueConverter;
  private final VariableValueIdPair<T> holder = new VariableValueIdPair<>(null, null);

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
    return fromBytes(byteBuffer);
  }

  @Override
  public VariableValueIdPair<T> fromBytes(ByteBuffer buffer) {
    final Long idIndex = buffer.getLong();
    final T varValue = _valueConverter.fromBytes(buffer);
    holder.setValue(varValue);
    holder.setIdIndex(idIndex);
    return holder;
  }

  @Override
  public int numBytes() {
    return Long.BYTES + _valueConverter.numBytes();
  }
}

