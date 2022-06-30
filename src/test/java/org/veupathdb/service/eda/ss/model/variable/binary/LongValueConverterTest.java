package org.veupathdb.service.eda.ss.model.variable.binary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LongValueConverterTest {

  @Test
  public void testNegativeNumber() {
    final LongValueConverter converter = new LongValueConverter();
    byte[] bytes = converter.toBytes(-6L);
    Assertions.assertEquals(-6L, converter.fromBytes(bytes));
  }

  @Test
  public void testLargerThanIntMax() {
    final LongValueConverter converter = new LongValueConverter();
    byte[] bytes = converter.toBytes(Integer.MAX_VALUE + 5L);
    Assertions.assertEquals(Integer.MAX_VALUE + 5L, converter.fromBytes(bytes));
  }
}
