package org.veupathdb.service.eda.ss.model.variable.binary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DoubleValueConverterTest {

  @Test
  public void testNegativeValue() {
    DoubleValueConverter converter = new DoubleValueConverter();
    byte[] arr = new DoubleValueConverter().toBytes(-6.0);
    Assertions.assertEquals(-6.0, converter.fromBytes(arr));
  }
}
