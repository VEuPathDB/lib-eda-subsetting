package org.veupathdb.service.edass.model;

public abstract class SubsetFilter {
  protected String entityPrimaryKeyColumunName;
  protected String entityTableName;

  public SubsetFilter(String entityPrimaryKeyColumunName, String entityTableName) {
    this.entityPrimaryKeyColumunName = entityPrimaryKeyColumunName;
    this.entityTableName = entityTableName;
  }

  public abstract String getSql();
}
