package org.veupathdb.service.eda.ss.model.variable.converter;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TupleSerializerTest {

  @Test
  public void testToAndFromBytes() {
    TupleSerializer<Long> tupleSerializer = new TupleSerializer<>(new LongValueConverter(), 3);
    byte[] bytes = tupleSerializer.toBytes(List.of(1L, 2L, 3L));
    MatcherAssert.assertThat(tupleSerializer.fromBytes(bytes), Matchers.contains(1L, 2L, 3L));
  }

  @Test
  public void testNonMatchedSize() {
    TupleSerializer<Long> tupleSerializer = new TupleSerializer<>(new LongValueConverter(), 5);
    Assertions.assertThrows(IllegalArgumentException.class, () -> tupleSerializer.toBytes(List.of(1L, 2L, 3L)));
  }
}
