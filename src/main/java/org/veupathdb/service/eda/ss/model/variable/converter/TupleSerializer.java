package org.veupathdb.service.eda.ss.model.variable.converter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TupleSerializer<T> implements BinarySerializer<List<T>> {
  private ValueConverter<T> converter;
  private int length;

  public TupleSerializer(ValueConverter<T> converter, int length) {
    this.converter = converter;
    this.length = length;
  }

  public int getLength() {
    return length;
  }

  @Override
  public byte[] toBytes(List<T> items) {
    if (items.size() != length) {
      throw new IllegalArgumentException("Can only convert a fixed length tuple of size " + length);
    }
    final ByteBuffer buffer = ByteBuffer.allocate(length * converter.numBytes());
    items.forEach(item -> buffer.put(converter.toBytes(item)));
    return buffer.array();
  }

  @Override
  public List<T> fromBytes(byte[] bytes) {
    final List<T> tuple = new ArrayList<>();
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    for (int i = 0; i < length; i++) {
      byte[] value = new byte[converter.numBytes()];
      buffer.get(value);
      tuple.add(converter.fromBytes(value));
    }
    return tuple;
  }

  @Override
  public int numBytes() {
    return length * converter.numBytes();
  }
}
