package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIStringSetFilter;

public class StringSetFilter extends Filter {

  private APIStringSetFilter inputFilter;
  
  public StringSetFilter(APIStringSetFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, inputFilter.getVariableId());
    this.inputFilter = inputFilter;
  }

  @Override
  public String getAndClausesSql() {
    return "AND string_value IN (" + String.join(", ", inputFilter.getStringSet()) + " )" + nl;
  }

}
