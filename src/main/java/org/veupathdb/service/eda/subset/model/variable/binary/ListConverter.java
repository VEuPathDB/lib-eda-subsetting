package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for a list of items.
 * @param <T> Type stored in list to serialize.
 */
public class ListConverter<T> implements BinaryConverter<List<T>> {
  private final BinaryConverter<T> converter;
  private final int size;

  public ListConverter(BinaryConverter<T> converter, int size) {
    this.converter = converter;
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  @Override
  public byte[] toBytes(List<T> items) {
    if (items.size() != size) {
      throw new IllegalArgumentException("Can only convert a fixed length list of size " + size);
    }
    final ByteBuffer buffer = ByteBuffer.allocate(size * converter.numBytes());
    items.forEach(item -> buffer.put(converter.toBytes(item)));
    return buffer.array();
  }

  @Override
  public List<T> fromBytes(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return fromBytes(buffer);
  }

  @Override
  public List<T> fromBytes(byte[] bytes, int offset) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.position(offset);
    return fromBytes(buffer);
  }

  @Override
  public List<T> fromBytes(ByteBuffer buffer) {
    final List<T> tuple = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      tuple.add(converter.fromBytes(buffer));
    }
    return tuple;
  }

  @Override
  public int numBytes() {
    return size * converter.numBytes();
  }
}
