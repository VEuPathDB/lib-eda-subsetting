package org.veupathdb.service.eda.ss.model.varcollection;

import org.veupathdb.service.eda.ss.model.variable.StringVariable;

import java.util.List;

public class StringVarCollection extends VarCollection<byte[], StringVariable> {

  public StringVarCollection(Properties properties) {
    super(properties);
  }

  @Override
  protected void assignDistributionDefaults(List<StringVariable> memberVars) {

  }
}
