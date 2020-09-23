package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APINumberRangeFilter;

public class NumberRangeFilter extends Filter {

  private APINumberRangeFilter inputFilter;
  
  public NumberRangeFilter(APINumberRangeFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, inputFilter.getVariableId());
    this.inputFilter = inputFilter;
  }

  @Override
  public String getAndClausesSql() {
    return "AND number_value >= " + inputFilter.getMin() + " AND number_value <= " + inputFilter.getMin() + nl;
  }

}
