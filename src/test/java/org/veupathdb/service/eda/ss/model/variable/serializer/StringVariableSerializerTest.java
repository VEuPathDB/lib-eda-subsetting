package org.veupathdb.service.eda.ss.model.variable.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringVariableSerializerTest {

  @Test
  public void testNumBytes() {
    int numBytes = 100;
    StringValueSerializer serializer = new StringValueSerializer(100);
    Assertions.assertEquals(numBytes, serializer.numBytes());
  }

  @Test
  public void testSerializeStringTooLarge() {
    StringValueSerializer serializer = new StringValueSerializer(5);
    Assertions.assertThrows(RuntimeException.class, () -> serializer.toBytes("Too long!"));
  }

  @Test
  public void testSerializeAndDeserializeWithPadding() {
    StringValueSerializer serializer = new StringValueSerializer(100);
    final String expected = "short.";
    byte[] arr = serializer.toBytes(expected);
    Assertions.assertEquals(100, arr.length);
    Assertions.assertEquals(expected, serializer.fromBytes(arr));
  }
}
