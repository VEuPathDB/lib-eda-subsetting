package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;

public class LongValueConverter implements BinaryConverter<Long> {
  private static final long BIT_MASK = 0xFF;

  @Override
  public byte[] toBytes(Long varValue) {
    return ByteBuffer.allocate(Long.BYTES).putLong(varValue).array();
  }

  @Override
  public Long fromBytes(byte[] bytes) {
    return fromBytes(bytes, 0);
  }

  @Override
  public Long fromBytes(byte[] bytes, int offset) {
    long result = 0;
    for (int i = offset; i < offset + Long.BYTES; i++) {
      result <<= 8; // Shift one byte, 8 bits.
      result |= (bytes[i] & BIT_MASK);
    }
    return result;
  }

  @Override
  public Long fromBytes(ByteBuffer buffer) {
    return buffer.getLong();
  }

  @Override
  public int numBytes() {
    return Long.BYTES;
  }

}
