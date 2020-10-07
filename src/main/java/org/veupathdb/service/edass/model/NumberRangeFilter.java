package org.veupathdb.service.edass.model;

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
    return "  AND number_value >= " + min + " AND number_value <= " + max + nl;
  }

}
