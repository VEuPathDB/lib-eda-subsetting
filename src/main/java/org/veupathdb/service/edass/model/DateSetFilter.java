package org.veupathdb.service.edass.model;

import java.util.List;

public class DateSetFilter extends Filter {

  private List<String> dateSet;
   
  public DateSetFilter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableId, List<String> dateSet) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, variableId);
    this.dateSet = dateSet;
  }

  @Override
  public String getAndClausesSql() {
    return "AND date_value IN (" + String.join(", ", dateSet) + " )" + nl;
  }

}
