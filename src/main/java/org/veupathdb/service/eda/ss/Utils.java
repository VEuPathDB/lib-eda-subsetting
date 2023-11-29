package org.veupathdb.service.eda.ss;

import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Utils {
  private static final int BYTE_COUNT_FOR_DIGIT_AND_EXP_CHAR = 2;
  private static final int MAX_DIGITS_BEFORE_SCIENTIFIC_NOTATION = 7;
  private static final long BIT_MASK = 0xFF;

  // TODO: remove once the client is fixed to not send in trailing 'Z'
  public static String standardizeLocalDateTime(String dateTimeString) {
    return (dateTimeString == null || !dateTimeString.endsWith("Z"))
        ? dateTimeString
        : dateTimeString.substring(0, dateTimeString.length() - 1);
  }

  public static int getPaddedUtf8StringLength(byte[] utf8Bytes) {
    int length = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      length <<= 8; // Shift one byte, 8 bits.
      length |= (utf8Bytes[i] & BIT_MASK);
    }
    return length;
  }

  public static byte[] trimPaddedBinary(byte[] paddedUtf8Bytes) {
    int length = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      length <<= 8; // Shift one byte, 8 bits.
      length |= (paddedUtf8Bytes[i] & BIT_MASK);
    }
    return Arrays.copyOfRange(paddedUtf8Bytes, Integer.BYTES, Integer.BYTES + length);
  }

  public static byte[] quotePaddedBinary(byte[] paddedUtf8Bytes) {
    byte[] utf8Bytes = trimPaddedBinary(paddedUtf8Bytes);
    byte[] quoted = new byte[utf8Bytes.length + 2];
    quoted[0] = '"';
    quoted[quoted.length - 1] = '"';
    System.arraycopy(utf8Bytes, 0, quoted, 1, utf8Bytes.length);
    return quoted;
  }

  public static StringValueConverter getFloatingPointUtf8Converter(int rangeMax, int precision) {
    // Floating point values are greater than 1e7 are displayed in scientific notation. For this reason, the maximum
    // size of our string is our precision + 3 bytes for the integer part of the decimal, the "e" in scientific notation
    // and the integer part of our value. We also reserve 4 bytes for the size of the padded string.
    int integerPartBytes = Integer.toString(rangeMax).getBytes(StandardCharsets.UTF_8).length; // Reserve integral part
    int numBytesReservedForIntPart = Math.min(integerPartBytes, MAX_DIGITS_BEFORE_SCIENTIFIC_NOTATION); // After 7 digits, we start using scientific notation
    int bytesReservedForIntegerPartOrScientificNotation = Math.max(numBytesReservedForIntPart, BYTE_COUNT_FOR_DIGIT_AND_EXP_CHAR);
    return new StringValueConverter(Integer.BYTES // Reserved for all padded strings
        + bytesReservedForIntegerPartOrScientificNotation // Reserved for integer part and/or left part of scientific notation.
        + precision // Plus space for decimal part.
        + 2); // Decimal point and minus sign.
  }
}
