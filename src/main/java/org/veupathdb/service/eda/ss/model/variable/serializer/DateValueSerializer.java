package org.veupathdb.service.eda.ss.model.variable.serializer;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;

public class DateValueSerializer implements ValueSerializer<LocalDateTime> {

  @Override
  public byte[] toBytes(LocalDateTime varValue) {
    final short year = (short) varValue.getYear();
    final byte month = (byte) varValue.getMonth().getValue();
    final byte day = (byte) varValue.getDayOfMonth();
    final byte hour = (byte) varValue.getHour();
    final byte minute = (byte) varValue.getMinute();
    // TODO What to truncate to? Minute, second, something else?
    return ByteBuffer.allocate(numBytes())
        .putShort(year)
        .put(month)
        .put(day)
        .put(hour)
        .put(minute)
        .array();
  }

  @Override
  public LocalDateTime fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    short year = buffer.getShort();
    byte month = buffer.get();
    byte day = buffer.get();
    byte hour = buffer.get();
    byte minute = buffer.get();
    return LocalDateTime.of(year, month, day, hour, minute);
  }

  @Override
  public int numBytes() {
    return 4 + Short.BYTES;
  }

}
