package org.veupathdb.service.eda.subset.model.variable;

public class VariablesCategory extends Variable {

  public VariablesCategory(Variable.Properties properties) {
    super(properties);
  }

  @Override
  public boolean hasValues() {
    return false;
  }
}
