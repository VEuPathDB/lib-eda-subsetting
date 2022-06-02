package org.veupathdb.service.eda.ss.model.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;
import org.veupathdb.service.eda.ss.testutil.TestDataProvider;


public class NumberRangeFilterTest {

  @Test
  public void testIntegerFilter() {
    final Entity entity = TestDataProvider.constructEntity();
    final IntegerVariable integerVariable = TestDataProvider.constructIntVariable(entity);
    entity.addVariable(integerVariable);
    NumberRangeFilter<Long> filter = new NumberRangeFilter<>("unit-test", entity, integerVariable, 0L, 100L);
    Assertions.assertTrue(filter.getPredicate().test(50L));
    Assertions.assertTrue(filter.getPredicate().test(0L));
    Assertions.assertTrue(filter.getPredicate().test(100L));
    Assertions.assertFalse(filter.getPredicate().test(120L));
    Assertions.assertFalse(filter.getPredicate().test(-1L));
  }
}