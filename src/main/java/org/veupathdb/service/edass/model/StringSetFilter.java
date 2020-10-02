package org.veupathdb.service.edass.model;

import java.util.List;

public class StringSetFilter extends Filter {

  private List<String> stringSet;
  
  public StringSetFilter(String entityId, String entityPrimaryKeyColumunName, String entityTableName, String variableId, List<String> stringSet) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, variableId);
    this.stringSet = stringSet;
  }

  @Override
  public String getAndClausesSql() {
    return "AND string_value IN (" + String.join(", ", stringSet) + " )" + nl;
  }

}
