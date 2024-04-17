package org.veupathdb.service.eda.subset.model.variable.binary;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

/**
 * Serializer for a list of items.
 * @param <T> Type stored in list to serialize.
 */
public class ArrayConverter<T> implements BinaryConverter<T[]> {
  private BinaryConverter<T> converter;
  private int size;
  private Class<T> clazz;

  public ArrayConverter(BinaryConverter<T> converter, int size, Class<T> clazz) {
    this.converter = converter;
    this.size = size;
    this.clazz = clazz;
  }

  public int getSize() {
    return size;
  }

  @Override
  public byte[] toBytes(T[] items) {
    if (items.length != size) {
      throw new IllegalArgumentException("Can only convert a fixed length list of size " + size);
    }
    final ByteBuffer buffer = ByteBuffer.allocate(size * converter.numBytes());
    for (T item: items) {
      buffer.put(converter.toBytes(item));
    }
    return buffer.array();
  }

  @Override
  public T[] fromBytes(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return fromBytes(buffer);
  }

  @Override
  public T[] fromBytes(byte[] bytes, int offset) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    buffer.position(offset);
    return fromBytes(buffer);
  }

  @Override
  public T[] fromBytes(ByteBuffer buffer) {
    final T[] tuple = (T[]) Array.newInstance(clazz, size);
    for (int i = 0; i < size; i++) {
      tuple[i] = converter.fromBytes(buffer);
    }
    return tuple;
  }

  @Override
  public int numBytes() {
    return size * converter.numBytes();
  }
}
