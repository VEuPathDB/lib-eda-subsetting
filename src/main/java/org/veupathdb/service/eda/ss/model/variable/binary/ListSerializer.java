package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for a list of items.
 * @param <T> Type stored in list to serialize.
 */
public class ListSerializer<T> implements BinaryConverter<List<T>> {
  private BinaryConverter<T> converter;
  private int size;

  public ListSerializer(BinaryConverter<T> converter, int size) {
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
    final List<T> tuple = new ArrayList<>();
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    for (int i = 0; i < size; i++) {
      byte[] value = new byte[converter.numBytes()];
      buffer.get(value);
      tuple.add(converter.fromBytes(value));
    }
    return tuple;
  }

  @Override
  public int numBytes() {
    return size * converter.numBytes();
  }
}
