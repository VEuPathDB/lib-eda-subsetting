package org.veupathdb.service.edass.model;

public class NumberRangeFilter extends Filter {

  private Number min;
  private Number max;
  
  public NumberRangeFilter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableId, Number min, Number max) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, variableId);
    this.min = min;
    this.max = max;
  }

  @Override
  public String getAndClausesSql() {
    return "  AND number_value >= " + min + " AND number_value <= " + max + nl;
  }

}
