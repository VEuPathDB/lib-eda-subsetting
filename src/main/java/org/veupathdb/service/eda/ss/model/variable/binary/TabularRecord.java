package org.veupathdb.service.eda.ss.model.variable.binary;

import java.util.List;

public class TabularRecord {
  private String entityId;
  private List<String> ancestorIds;
  private List<Object> varValues;

  public TabularRecord(String entityId, List<String> ancestorIds, List<Object> varValues) {
    this.entityId = entityId;
    this.ancestorIds = ancestorIds;
    this.varValues = varValues;
  }

  public String getEntityId() {
    return entityId;
  }

  public List<String> getAncestorIds() {
    return ancestorIds;
  }

  public List<Object> getVarValues() {
    return varValues;
  }

  public static class ColumnDef<T> {
    public ColumnDef(Class<T> columnClass, String columnName) {

    }
  }
}
