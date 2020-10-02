package org.veupathdb.service.edass.model;

public class DateRangeFilter extends Filter {

  private String min;
  private String max;
  
  public DateRangeFilter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableId, String min, String max) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, variableId);
    this.min = min;
    this.max = max;
  }

  @Override
  public String getAndClausesSql() {
    return "AND date_value >= " + min + " AND date_value <= " + max + nl;
  }
}
