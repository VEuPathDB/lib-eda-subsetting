package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;
import org.veupathdb.service.eda.ss.model.Entity;

public class StringVariable extends VariableWithValues {

  public StringVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties) {
    super(varProperties, valueProperties);
    validateType(VariableType.STRING);
  }
}
