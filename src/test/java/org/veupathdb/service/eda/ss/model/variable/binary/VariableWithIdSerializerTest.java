package org.veupathdb.service.eda.ss.model.variable.binary;

import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import static org.junit.jupiter.api.Assertions.*;

public class VariableWithIdSerializerTest {

  @Test
  public void testToAndFromBytes() {
    final VariableValueIdPair<Long> expected = new VariableValueIdPair<>(100L, 1000L);
    ValueWithIdSerializer<Long> ser = new ValueWithIdSerializer<>(new LongValueConverter());
    ValueWithIdDeserializer<Long> deser = new ValueWithIdDeserializer<>(new LongValueConverter());
    byte[] serialized = ser.toBytes(expected);
    VariableValueIdPair<Long> deserialized = deser.fromBytes(serialized);
    assertEquals(expected, deserialized);
  }

}