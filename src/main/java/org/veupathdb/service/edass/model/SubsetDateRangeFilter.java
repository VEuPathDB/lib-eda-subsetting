package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.DateRangeFilter;

public class SubsetDateRangeFilter extends SubsetFilter {

  private DateRangeFilter inputFilter;
  
  public SubsetDateRangeFilter(DateRangeFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
