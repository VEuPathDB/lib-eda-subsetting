package org.veupathdb.service.edass.model;

import java.util.ArrayList;
import java.util.List;

public abstract class Filter {
  protected Entity entity;
  protected String variableName;
  
  protected static final String nl = System.lineSeparator();

  public Filter(Entity entity, String variableName) {
    this.entity = entity;
    this.variableName = variableName;
  }

  public String getSql() {
    List<String> selectColsList = new ArrayList<String>(entity.getAncestorPkColNames());
    selectColsList.add(entity.getEntityPKColName());
    String selectCols = String.join(", ", selectColsList);
    
    return "  SELECT " + selectCols + " FROM " + entity.getEntityTallTableName() + nl
        + "  WHERE ontology_term_name = '" + variableName + "'" + nl 
        + getAndClausesSql();
  }

  /**
   * subclasses provide AND clauses specific to their type
   * @return
   */
  public abstract String getAndClausesSql();

  public Entity getEntity() {
    return entity;
  }
  
  
}
