package org.veupathdb.service.eda.ss.model.variable.converter;

import org.gusdb.fgputil.FormatUtil;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DateValueConverter implements ValueConverter<LocalDateTime> {

  @Override
  public LocalDateTime fromString(String s) {
    return FormatUtil.parseDateTime(s);
  }

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
  public int numBytes() {
    return Long.BYTES;
  }

}
