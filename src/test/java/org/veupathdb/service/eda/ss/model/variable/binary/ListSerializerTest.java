package org.veupathdb.service.eda.ss.model.variable.binary;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ListSerializerTest {

  @Test
  public void testToAndFromBytes() {
    ListSerializer<Long> listSerializer = new ListSerializer<>(new LongValueConverter(), 3);
    byte[] bytes = listSerializer.toBytes(List.of(1L, 2L, 3L));
    MatcherAssert.assertThat(listSerializer.fromBytes(bytes), Matchers.contains(1L, 2L, 3L));
  }

  @Test
  public void testNonMatchedSize() {
    ListSerializer<Long> listSerializer = new ListSerializer<>(new LongValueConverter(), 5);
    Assertions.assertThrows(IllegalArgumentException.class, () -> listSerializer.toBytes(List.of(1L, 2L, 3L)));
  }
}
