package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIDateRangeFilter;

public class DateRangeFilter extends Filter {

  private APIDateRangeFilter inputFilter;
  
  public DateRangeFilter(APIDateRangeFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, inputFilter.getVariableId());
    this.inputFilter = inputFilter;
  }

  @Override
  public String getAndClausesSql() {
    return "AND date_value >= " + inputFilter.getMin() + " AND date_value <= " + inputFilter.getMin() + nl;
  }
}
