package org.veupathdb.service.eda.ss.model.variable;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.junit.jupiter.api.Assertions.*;

public class LongitudeVariableTest {

  @Test
  public void valueToString() {
    BigDecimal bd = BigDecimal.valueOf(1.66555555555555);
    bd = bd.setScale(Math.min(bd.precision() - 1, 10), RoundingMode.HALF_UP);
    System.out.println(bd);
  }
}