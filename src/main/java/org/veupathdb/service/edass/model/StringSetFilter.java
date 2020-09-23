package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.APIStringSetFilter;

public class StringSetFilter extends Filter {

  private APIStringSetFilter inputFilter;
  
  public StringSetFilter(APIStringSetFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
