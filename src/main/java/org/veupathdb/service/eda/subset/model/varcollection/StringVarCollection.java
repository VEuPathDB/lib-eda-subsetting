package org.veupathdb.service.eda.subset.model.varcollection;

import org.veupathdb.service.eda.subset.model.variable.StringVariable;

import java.util.List;

public class StringVarCollection extends VarCollection<byte[], StringVariable> {

  public StringVarCollection(Properties properties) {
    super(properties);
  }

  @Override
  protected void assignDistributionDefaults(List<StringVariable> memberVars) {
    // No distribution configuration for StringVariables.
  }
}
