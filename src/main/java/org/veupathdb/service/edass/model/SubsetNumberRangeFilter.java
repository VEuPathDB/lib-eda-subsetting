package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.NumberRangeFilter;

public class SubsetNumberRangeFilter extends SubsetFilter {

  private NumberRangeFilter inputFilter;
  
  public SubsetNumberRangeFilter(NumberRangeFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
