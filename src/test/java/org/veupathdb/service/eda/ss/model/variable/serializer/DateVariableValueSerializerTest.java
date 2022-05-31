package org.veupathdb.service.eda.ss.model.variable.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class DateVariableValueSerializerTest {
  private DateValueSerializer serializer = new DateValueSerializer();

  @Test
  public void testSerializeAndDeserialize() {
    final LocalDateTime expected = LocalDateTime.now();
    final byte[] bytes = serializer.toBytes(expected);
    final LocalDateTime deserialized = serializer.fromBytes(bytes);
    // TODO: Are minutes the correct truncation?
    Assertions.assertEquals(expected.truncatedTo(ChronoUnit.MINUTES), deserialized);
  }
}
