package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.NumberSetFilter;

public class SubsetNumberSetFilter extends SubsetFilter {

  private NumberSetFilter inputFilter;
  
  public SubsetNumberSetFilter(NumberSetFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
