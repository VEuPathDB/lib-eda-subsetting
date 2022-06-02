package org.veupathdb.service.eda.ss.model.variable.converter;

import java.nio.ByteBuffer;

public class LongValueConverter implements ValueConverter<Long> {

  @Override
  public Long fromString(String s) {
    return Long.parseLong(s);
  }

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
