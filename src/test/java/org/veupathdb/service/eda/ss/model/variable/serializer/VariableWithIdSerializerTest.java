package org.veupathdb.service.eda.ss.model.variable.serializer;

import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValue;

import static org.junit.jupiter.api.Assertions.*;

public class VariableWithIdSerializerTest {

  @Test
  public void testToAndFromBytes() {
    final VariableValue<Integer> expected = new VariableValue<>(100, 1000);
    ValueWithIdSerializer<Integer> ser = new ValueWithIdSerializer<>(new IntValueSerializer());
    byte[] serialized = ser.convertToBytes(expected);
    VariableValue<Integer> deserialized = ser.convertFromBytes(serialized);
    assertEquals(expected, deserialized);
  }

}