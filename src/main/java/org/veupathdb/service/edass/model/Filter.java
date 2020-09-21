package org.veupathdb.service.edass.model;

public abstract class Filter {
  protected String entityPrimaryKeyColumunName;
  protected String entityTableName;

  public Filter(String entityPrimaryKeyColumunName, String entityTableName) {
    this.entityPrimaryKeyColumunName = entityPrimaryKeyColumunName;
    this.entityTableName = entityTableName;
  }

  public abstract String getSql();
}
