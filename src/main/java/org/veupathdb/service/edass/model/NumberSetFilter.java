package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APINumberSetFilter;

public class NumberSetFilter extends Filter {

  private APINumberSetFilter inputFilter;
  
  public NumberSetFilter(APINumberSetFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
