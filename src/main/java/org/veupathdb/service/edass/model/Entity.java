/**
 * 
 */
package org.veupathdb.service.edass.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Steve
 *
 */
public class Entity {
  private String entityId;
  private String entityName;
  private String entityTallTableName;
  private String entityAncestorTableName;
  private String entityPrimaryKeyColumnName;
  private List<String> ancestorPkColNames = new ArrayList<String>();
  private List<String> ancestorFullPkColNames; // entityName.pkColName
  
  public Entity(String entityName, String entityId, String entityTallTableName, String entityAncestorTableName,
      String entityPrimaryKeyColumnName) {
    this.entityTallTableName = entityTallTableName;
    this.entityAncestorTableName = entityAncestorTableName;
    this.entityPrimaryKeyColumnName = entityPrimaryKeyColumnName;
    this.ancestorFullPkColNames = ancestorPkColNames.stream().map(pk -> entityName + "." + pk).collect(Collectors.toList());
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
  
  public String getEntityAncestorTableName() {
    return entityAncestorTableName;
  }
  
  public List<String> getAncestorPkColNames() {
    return Collections.unmodifiableList(ancestorPkColNames);
  }
  
  public List<String> getAncestorFullPkColNames() {
    return Collections.unmodifiableList(ancestorFullPkColNames);
  }

}
