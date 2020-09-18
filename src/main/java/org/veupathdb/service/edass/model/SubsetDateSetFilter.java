package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.DateSetFilter;

public class SubsetDateSetFilter extends SubsetFilter {

  private DateSetFilter inputFilter;
   
  public SubsetDateSetFilter(DateSetFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
