package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;

public class NumberRangeFilter extends Filter {

  private APINumberRangeFilter inputFilter;
  
  public NumberRangeFilter(APINumberRangeFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
