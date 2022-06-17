package org.veupathdb.service.eda.ss.model.variable.binary;

import java.nio.ByteBuffer;

public class LongValueConverter implements BinaryConverter<Long> {

  @Override
  public byte[] toBytes(Long varValue) {
    return ByteBuffer.allocate(Long.BYTES).putLong(varValue).array();
  }

  @Override
  public Long fromBytes(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getLong();
  }

  @Override
  public int numBytes() {
    return Long.BYTES;
  }

}
