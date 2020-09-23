package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIDateSetFilter;

public class DateSetFilter extends Filter {

  private APIDateSetFilter inputFilter;
   
  public DateSetFilter(APIDateSetFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
