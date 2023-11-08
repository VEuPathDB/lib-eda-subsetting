package org.veupathdb.service.eda.subset.model.variable.binary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringValueConverterTest {

  @Test
  public void testNumBytes() {
    int numBytes = 100;
    StringValueConverter serializer = new StringValueConverter(100);
    Assertions.assertEquals(numBytes, serializer.numBytes());
  }

  @Test
  public void testSerializeStringTooLarge() {
    StringValueConverter serializer = new StringValueConverter(5);
    Assertions.assertThrows(RuntimeException.class, () -> serializer.toBytes("Too long!"));
  }

  @Test
  public void testSerializeAndDeserializeWithPadding() {
    StringValueConverter serializer = new StringValueConverter(100);
    final String expected = "short.";
    byte[] arr = serializer.toBytes(expected);
    Assertions.assertEquals(100, arr.length);
    Assertions.assertEquals(expected, serializer.fromBytes(arr));
  }
}
