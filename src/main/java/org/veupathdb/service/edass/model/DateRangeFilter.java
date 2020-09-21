package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;

public class DateRangeFilter extends Filter {

  private APIDateRangeFilter inputFilter;
  
  public DateRangeFilter(APIDateRangeFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
