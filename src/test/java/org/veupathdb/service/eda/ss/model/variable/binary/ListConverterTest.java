package org.veupathdb.service.eda.ss.model.variable.binary;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ListConverterTest {

  @Test
  public void testToAndFromBytes() {
    ListConverter<Long> listConverter = new ListConverter<>(new LongValueConverter(), 3);
    byte[] bytes = listConverter.toBytes(List.of(1L, 2L, 3L));
    MatcherAssert.assertThat(listConverter.fromBytes(bytes), Matchers.contains(1L, 2L, 3L));
  }

  @Test
  public void testNonMatchedSize() {
    ListConverter<Long> listConverter = new ListConverter<>(new LongValueConverter(), 5);
    Assertions.assertThrows(IllegalArgumentException.class, () -> listConverter.toBytes(List.of(1L, 2L, 3L)));
  }
}
