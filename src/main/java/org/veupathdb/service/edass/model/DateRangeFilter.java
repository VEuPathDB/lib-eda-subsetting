package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;

public class DateRangeFilter extends Filter {

  private LocalDateTime min;
  private LocalDateTime max;
  
  public DateRangeFilter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableId, LocalDateTime min, LocalDateTime max) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, variableId);
    this.min = min;
    this.max = max;
  }

  @Override
  public String getAndClausesSql() {
    return "AND date_value >= " + min + " AND date_value <= " + max + nl;
  }
}
