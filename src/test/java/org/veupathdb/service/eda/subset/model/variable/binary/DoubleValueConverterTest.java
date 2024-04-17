package org.veupathdb.service.eda.subset.model.variable.binary;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DoubleValueConverterTest {

  @Test
  public void testNegativeValue() {
    DoubleValueConverter converter = new DoubleValueConverter();
    byte[] arr = new DoubleValueConverter().toBytes(-6.0);
    Assertions.assertEquals(-6.0, converter.fromBytes(arr));
  }

  @Test
  public void testRandomValues() {
    DoubleValueConverter converter = new DoubleValueConverter();
    byte[] arr = new DoubleValueConverter().toBytes(3.6);
    Assertions.assertEquals(3.6, converter.fromBytes(arr));
  }

}
