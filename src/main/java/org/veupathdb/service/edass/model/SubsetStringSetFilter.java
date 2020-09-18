package org.veupathdb.service.edass.model;

import org.veupathdb.service.edass.generated.model.StringSetFilter;

public class SubsetStringSetFilter extends SubsetFilter {

  private StringSetFilter inputFilter;
  
  public SubsetStringSetFilter(StringSetFilter inputFilter, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityPrimaryKeyColumunName, entityTableName);
    this.inputFilter = inputFilter;
  }

  @Override
  public String getSql() {
    // TODO Auto-generated method stub
    return null;
  }

}
