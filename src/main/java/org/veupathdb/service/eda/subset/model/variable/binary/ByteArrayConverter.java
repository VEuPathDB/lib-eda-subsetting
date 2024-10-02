package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;

public record ByteArrayConverter(int numBytes) implements BinaryConverter<byte[]> {
  @Override
  public byte[] fromBytes(byte[] bytes) {
    return bytes;
  }

  @Override
  public byte[] fromBytes(byte[] bytes, int offset) {
    return fromBytes(ByteBuffer.wrap(bytes).position(offset));
  }

  @Override
  public byte[] fromBytes(ByteBuffer buffer) {
    byte[] bytes = new byte[numBytes];
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public byte[] toBytes(byte[] varValue) {
    return varValue;
  }
}
