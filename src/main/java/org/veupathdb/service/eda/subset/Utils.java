package org.veupathdb.service.eda.subset;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Utils {
  private static final int BIT_MASK = 0xFF;

  // TODO: remove once the client is fixed to not send in trailing 'Z'
  public static String standardizeLocalDateTime(String dateTimeString) {
    return (dateTimeString == null || !dateTimeString.endsWith("Z"))
        ? dateTimeString
        : dateTimeString.substring(0, dateTimeString.length() - 1);
  }

  public static int getPaddedUtf8StringLength(byte[] utf8Bytes) {
    return getLengthFromPaddedString(utf8Bytes);
  }

  public static byte[] trimPaddedBinary(byte[] paddedUtf8Bytes) {
    int length = getLengthFromPaddedString(paddedUtf8Bytes);
    return Arrays.copyOfRange(paddedUtf8Bytes, Integer.BYTES, Integer.BYTES + length);
  }

  public static String decodePaddedUtf8Binary(byte[] paddedUtf8Bytes) {
    int length = getLengthFromPaddedString(paddedUtf8Bytes);
    return new String(paddedUtf8Bytes, 4, length, StandardCharsets.UTF_8);
  }

  private static int getLengthFromPaddedString(byte[] paddedUtf8Bytes) {
    int length = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      length <<= 8; // Shift one byte, 8 bits.
      length |= (paddedUtf8Bytes[i] & BIT_MASK);
    }
    return length;
  }

  public static byte[] quotePaddedBinary(byte[] paddedUtf8Bytes) {
    byte[] utf8Bytes = trimPaddedBinary(paddedUtf8Bytes);
    byte[] quoted = new byte[utf8Bytes.length + 2];
    quoted[0] = '"';
    quoted[quoted.length - 1] = '"';
    System.arraycopy(utf8Bytes, 0, quoted, 1, utf8Bytes.length);
    return quoted;
  }
}
