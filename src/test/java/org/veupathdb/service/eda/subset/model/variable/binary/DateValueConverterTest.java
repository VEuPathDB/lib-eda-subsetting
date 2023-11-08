package org.veupathdb.service.eda.subset.model.variable.binary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class DateValueConverterTest {
  private final DateValueConverter serializer = new DateValueConverter();

  @Test
  public void testSerializeAndDeserialize() {
    final LocalDateTime expected = LocalDateTime.now();
    final byte[] bytes = serializer.toBytes(expected);
    final LocalDateTime deserialized = serializer.fromBytes(bytes);
    Assertions.assertEquals(expected.truncatedTo(ChronoUnit.MILLIS), deserialized);
  }
}
