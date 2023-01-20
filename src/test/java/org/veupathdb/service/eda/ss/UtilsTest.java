package org.veupathdb.service.eda.ss;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class UtilsTest {

  @Test
  void trimPaddedBinary() {
    final int allocatedStringLength = 32;
    ByteBuffer buffer = ByteBuffer.allocate(allocatedStringLength);
    buffer.putInt(8);
    buffer.put("mystring".getBytes(StandardCharsets.UTF_8));
    buffer.position(0);
    buffer.limit(allocatedStringLength);
    byte[] bytes = new byte[allocatedStringLength];
    buffer.get(bytes);
    Assertions.assertEquals("mystring", new String(Utils.trimPaddedBinary(bytes), StandardCharsets.UTF_8));
  }

  @Test
  void quotePaddedBinary() {
    final int allocatedStringLength = 32;
    final String expectedString = "mystring";
    ByteBuffer buffer = ByteBuffer.allocate(allocatedStringLength);
    buffer.putInt(expectedString.length());
    buffer.put(expectedString.getBytes(StandardCharsets.UTF_8));
    buffer.position(0);
    buffer.limit(allocatedStringLength);
    byte[] bytes = new byte[allocatedStringLength];
    buffer.get(bytes);
    Assertions.assertEquals("\"mystring\"", new String(Utils.quotePaddedBinary(bytes), StandardCharsets.UTF_8));
  }
}