package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;

public class ByteArrayConverter implements BinaryConverter<byte[]> {
  private int numBytes;

  public ByteArrayConverter(int numBytes) {
    this.numBytes = numBytes;
  }

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
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public byte[] toBytes(byte[] varValue) {
    return varValue;
  }

  @Override
  public int numBytes() {
    return numBytes;
  }
}
