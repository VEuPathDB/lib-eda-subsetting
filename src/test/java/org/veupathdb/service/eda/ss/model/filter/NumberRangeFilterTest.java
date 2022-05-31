package org.veupathdb.service.eda.ss.model.filter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableType;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import java.util.Collections;

public class NumberRangeFilterTest {
  private static String ENTITY_ID = "entity";
  private static String STUDY_ID = "study";
  private static String VARIABLE_ID = "var";

  @Test
  public void testIntegerFilter() {
    final Entity entity = constructEntity();
    final IntegerVariable integerVariable = constructIntVariable(entity);
    entity.addVariable(integerVariable);
    NumberRangeFilter<Long> filter = new NumberRangeFilter<>("unit-test", entity, integerVariable, 0L, 100L);
    Assertions.assertTrue(filter.getPredicate().test(50L));
    Assertions.assertTrue(filter.getPredicate().test(0L));
    Assertions.assertTrue(filter.getPredicate().test(100L));
    Assertions.assertFalse(filter.getPredicate().test(120L));
    Assertions.assertFalse(filter.getPredicate().test(-1L));
  }

  private Entity constructEntity() {
    return new Entity(
        ENTITY_ID,
        STUDY_ID,
        "My Study",
        "My Studies",
        "My favority study",
        "Mine",
        0L,
        false,
        false);
  }

  private IntegerVariable constructIntVariable(Entity entity) {
    return new IntegerVariable(
        new Variable.Properties(
            "label",
            VARIABLE_ID,
            entity,
            null,
            null,
            0L,
            "50",
            null,
            null
        ),
        new VariableWithValues.Properties(
            VariableType.INTEGER,
            null,
            Collections.emptyList(),
            0L,
            false,
            false,
            false,
            false,
            false
        ),
        new NumberDistributionConfig<>(0L, 10L, 0L, 10L, 2L, 2L),
        new IntegerVariable.Properties("bleep bloops")
    );
  }
}