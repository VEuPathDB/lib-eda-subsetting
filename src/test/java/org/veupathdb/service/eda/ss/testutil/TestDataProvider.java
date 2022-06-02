package org.veupathdb.service.eda.ss.testutil;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.*;

import java.util.Collections;

public class TestDataProvider {
  public static final String ENTITY_ID = "EUPA_1024";
  public static final String STUDY_ID = "GEMS1A";
  public static final String VARIABLE_ID = "EUPA_1111";

  public static Entity constructEntity() {
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

  public static IntegerVariable constructIntVariable(Entity entity) {
    return new IntegerVariable(
        constructGenericVarProps(entity),
        constructVarValuesProps(VariableType.INTEGER),
        new NumberDistributionConfig<>(0L, 10L, 0L, 10L, 2L, 2L),
        new IntegerVariable.Properties("bleep bloops")
    );
  }

  public static DateVariable constructDateVariable(Entity entity) {
    return new DateVariable(
        constructGenericVarProps(entity),
        constructVarValuesProps(VariableType.DATE),
        new DateDistributionConfig(false, VariableDataShape.CONTINUOUS, null, null, "min", "max", 10, "days", null)
    );
  }

  private static Variable.Properties constructGenericVarProps(Entity entity) {
    return new Variable.Properties(
        "label",
        VARIABLE_ID,
        entity,
        null,
        null,
        0L,
        "50",
        null,
        null
    );
  }

  private static VariableWithValues.Properties constructVarValuesProps(VariableType variableType) {
    return new VariableWithValues.Properties(
        variableType,
        null,
        Collections.emptyList(),
        0L,
        false,
        false,
        false,
        false,
        false
    );
  }
}
