package org.veupathdb.service.eda.subset.model.variable.binary;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DateValueConverter implements BinaryConverter<LocalDateTime> {

  @Override
  public byte[] toBytes(LocalDateTime varValue) {
    long value = varValue.toInstant(ZoneOffset.UTC).toEpochMilli();
    return ByteBuffer.allocate(Long.BYTES)
        .putLong(value)
        .array();
  }

  @Override
  public LocalDateTime fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    long value = buffer.getLong();
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
  }

  @Override
  public LocalDateTime fromBytes(byte[] bytes, int offset) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes)
        .position(offset);
    long value = buffer.getLong();
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
  }

  @Override
  public LocalDateTime fromBytes(ByteBuffer buffer) {
    long value = buffer.getLong();
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
  }

  @Override
  public int numBytes() {
    return Long.BYTES;
  }

}
