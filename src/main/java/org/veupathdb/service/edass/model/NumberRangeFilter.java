package org.veupathdb.service.edass.model;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

public class NumberRangeFilter extends Filter {

  private Number min;
  private Number max;
  
  public NumberRangeFilter(Entity entity, String variableId, Number min, Number max) {
    super(entity, variableId);
    this.min = min;
    this.max = max;
  }

  @Override
  public String getAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " >= " + min + " AND " + NUMBER_VALUE_COL_NAME + " <= " + max + nl;
  }

}
