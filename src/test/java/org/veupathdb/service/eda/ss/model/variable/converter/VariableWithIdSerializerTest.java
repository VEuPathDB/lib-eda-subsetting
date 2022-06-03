package org.veupathdb.service.eda.ss.model.variable.converter;

import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import static org.junit.jupiter.api.Assertions.*;

public class VariableWithIdSerializerTest {

  @Test
  public void testToAndFromBytes() {
    final VariableValueIdPair<Long> expected = new VariableValueIdPair<>("100", 1000L);
    ValueWithIdSerializer<Long> ser = new ValueWithIdSerializer<>(new LongValueConverter());
    byte[] serialized = ser.convertToBytes(expected);
    VariableValueIdPair<Long> deserialized = ser.convertFromBytes(serialized);
    assertEquals(expected, deserialized);
  }

}