package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIDateSetFilter;

public class DateSetFilter extends Filter {

  private APIDateSetFilter inputFilter;
   
  public DateSetFilter(APIDateSetFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, inputFilter.getVariableId());
    this.inputFilter = inputFilter;
  }

  @Override
  public String getAndClausesSql() {
    return "AND date_value IN (" + String.join(", ", inputFilter.getDateSet()) + " )" + nl;
  }

}
