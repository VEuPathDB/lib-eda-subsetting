/**
 * 
 */
package org.veupathdb.service.edass.model;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Steve
 *
 */
public class SubsetEntity {
  private Set<SubsetFilter> filters;
  private String entityId;
  private String entityName;
  private String entityTallTableName;
  private String entityAncestorTableName;
  private String entityPrimaryKeyColumnName;
  
  
  
  private static final String nl = System.lineSeparator();

  public SubsetEntity(String entityName, String entityId, String entityTallTableName, String entityAncestorTableName,
      String entityPrimaryKeyColumnName, Set<SubsetFilter> filters) {
    this.filters = filters;
    this.entityTallTableName = entityTallTableName;
    this.entityAncestorTableName = entityAncestorTableName;
    this.entityPrimaryKeyColumnName = entityPrimaryKeyColumnName;
  }

  String getWithClauseSql() {
    
    // default WITH body assumes no filters.  we use the ancestor table because it is small
    String withBody = "SELECT " + getEntityPrimaryKeyColumnName() + " FROM " + entityAncestorTableName;

    if (!filters.isEmpty()) {
      Set<String> filterSqls = filters.stream().map(f -> f.getSql()).collect(Collectors.toSet());
      withBody = String.join(nl + "INTERSECT" + nl, filterSqls);
    }   
    
    return "WITH " + entityName + " as (" + nl
        + withBody + nl
        + ")";
  }
  
  // this join is formed using the name from the WITH clause, which is the entity name
  String getSqlJoinString(SubsetEntity entity) {
    return entity.getEntityName() + "." + entity.getEntityPrimaryKeyColumnName() +
     " = " + getEntityName() + "." + getEntityPrimaryKeyColumnName();
 }

  String getEntityId() {
    return entityId;
  }

  String getEntityName() {
    return entityName;
  }

  String getEntityTallTableName() {
    return entityTallTableName;
  }

  String getEntityPrimaryKeyColumnName() {
    return entityPrimaryKeyColumnName;
  }
}
