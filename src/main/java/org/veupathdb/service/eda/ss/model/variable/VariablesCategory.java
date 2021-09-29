package org.veupathdb.service.eda.ss.model.variable;

public class VariablesCategory extends Variable {

  public VariablesCategory(Variable.Properties properties) {
    super(properties);
  }

  @Override
  public boolean hasValues() {
    return false;
  }
}
