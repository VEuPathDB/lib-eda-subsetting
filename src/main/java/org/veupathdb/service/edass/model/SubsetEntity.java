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
  
  /*
   * definition: an active entity is one that must be included in the SQL
   * definition: an active subtree is one in which any entities in the subtree are active.
   * 
   * this entity is active if any of these apply:
   *   1. it has filters
   *   2. it is the output entity
   *   3. it is neither of the above, but has more than one child that is the root of an active subtree
   *   
   *   (criterion 3 lets us join elements across connected subtrees)
   *   
   *          ----X----
   *          |       |
   *        --I--     I
   *        |   |     |
   *        A   I     A
   *        
   * In the picture above the A entities are active and I are inactive.  
   * X has two children that are active subtrees.
   * We need to force X to be active so that we can join the lower A entities.
   *
   * So will we now have this:
   * 
   *          ----A----
   *          |       |
   *        --I--     I
   *        |   |     |
   *        A   I     A
   *        
   * Finally, we want to prune the tree of inactive nodes, so we have the minimal active tree:
   * 
   *        ----A----
   *        |       |
   *        A       A
   *        
   * Now we can ascend the tree and form the concise SQL joins we need
   * 
   * Using a concrete example:
   *         ----H----
   *         |       |
   *       --P--     E
   *       |   |     |
   *       O   S     T
   * 
   * If O and and T are active (have filters or are the output entity), then we ultimately need this join:
   *   where O.H_id = H.H_id
   *     and T.H_id = H.H_id
   * 
   */
  
  
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
