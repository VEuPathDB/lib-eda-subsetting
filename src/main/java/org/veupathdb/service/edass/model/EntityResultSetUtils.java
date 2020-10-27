package org.veupathdb.service.edass.model;

import java.sql.ResultSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;

import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

/**
 * utilities for creating Entities from result sets
 * @author Steve
 *
 */
public class EntityResultSetUtils {

  static TreeNode<Entity> getStudyEntityTree(DataSource datasource, String studyId) {
    
    String sql = generateEntityTreeSql(studyId);
    
    // entityID -> list of child entities
    Map<String, List<Entity>> simpleTree = new HashMap<String, List<Entity>>();
    
    Entity rootEntity = new SQLRunner(datasource, sql).executeQuery(rs -> {
      Entity root = null;
      while (rs.next()) {
        Entity entity = createEntityFromResultSet(rs);
        String parentId = rs.getString(ENTITY_PARENT_ID_COL_NAME);
        if (parentId == null) {
          if (root != null) throw new RuntimeException("In Study " + studyId + " found more than one root entity");
          root = entity;
        } else {
          if (!simpleTree.containsKey(parentId)) simpleTree.put(parentId, new ArrayList<Entity>());
          simpleTree.get(parentId).add(entity);
        }
      }
      return root;
    });
    
    if (rootEntity == null) throw new RuntimeException("Found no entities for study: " + studyId);

    List<Entity> rootKids = simpleTree.get(rootEntity.getId());
    TreeNode<Entity> rootNode = new TreeNode<Entity>(rootEntity);
    populateEntityTree(rootNode, rootKids, simpleTree);
    return rootNode;
  }
  
  static void populateEntityTree(TreeNode<Entity> parentNode, List<Entity> children, Map<String, List<Entity>> simpleTree) {
    for (Entity child : children) {
      TreeNode<Entity> childNode = new TreeNode<Entity>(child); 
      parentNode.addChildNode(childNode);
      
      // if this node has children, recurse
      if (simpleTree.containsKey(child.getId()))
        populateEntityTree(childNode, simpleTree.get(child.getId()), simpleTree);
    }
  }
  
  static String generateEntityTreeSql(String studyId) {
    String[] cols = {STUDY_ID_COL_NAME, NAME_COL_NAME, ENTITY_ID_COL_NAME, DESCRIP_COL_NAME, ENTITY_PARENT_ID_COL_NAME};
    return "SELECT " + String.join(", ", cols) + nl
        + "FROM " + ENTITY_TABLE_NAME + " e, " + ENTITY_NAME_TABLE_NAME + " n" + nl
        + "WHERE e." + ENTITY_NAME_ID_COL_NAME + " = n." + ENTITY_NAME_ID_COL_NAME + nl
        + "AND " + STUDY_ID_COL_NAME + " = '" + studyId + "'" + nl
        + "ORDER BY " + ENTITY_ID_COL_NAME;  // stable ordering supports unit testing
  }

  static Entity createEntityFromResultSet(ResultSet rs) {

    try {
      String name = getRsStringNotNull(rs, NAME_COL_NAME);
      String id = getRsStringNotNull(rs, ENTITY_ID_COL_NAME);
      String descrip = getRsStringNotNull(rs, DESCRIP_COL_NAME);
      
      return new Entity(name, id, descrip, id + "_tall", id + "_ancestors", name + "_id");
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Tall table rows look like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_id, string_value, number_value, date_value

   * @param rs
   * @return
   */
  static Map<String, String> resultSetToTallRowMap(Entity entity, ResultSet rs, List<String> olNames) {

    Map<String, String> tallRow = new HashMap<String, String>();

    try {
      for (String colName : entity.getAncestorPkColNames()) {
        tallRow.put(colName, rs.getString(colName));
      }
      tallRow.put(entity.getPKColName(), rs.getString(entity.getPKColName()));
      tallRow.put(VARIABLE_ID_COL_NAME, rs.getString(VARIABLE_ID_COL_NAME));
      
      Variable var = entity.getVariable(rs.getString(VARIABLE_ID_COL_NAME))
          .orElseThrow(() -> new RuntimeException("Can't find column in tall table result set: " + VARIABLE_ID_COL_NAME));

      tallRow.put(VARIABLE_VALUE_COL_NAME, var.getType().convertRowValueToStringValue(rs));
      
      return tallRow;
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  /**
   * Return a function that transforms a list of tall table rows to a single wide row.
   * 
   * Tall table rows look like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_id, value
   *   ancestor1_pk, ancestor2_pk, pk, variableB_id, value
   *   ancestor1_pk, ancestor2_pk, pk, variableC_id, value
   *   
   * Output wide row looks like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_value, variableB_value, variableC_value
   *   
   *   (all values are converted to strings)
   * @return
   */
  static Function<List<Map<String, String>>, Map<String, String>> getTallToWideFunction(Entity entity) {
    
    String errPrefix = "Tall row supplied to entity " + entity.getId();

    return tallRows -> {
      
      Map<String, String> wideRow = new HashMap<String, String>();

      String tallRowEnityId = tallRows.get(0).get(entity.getPKColName());
      wideRow.put(entity.getPKColName(), tallRowEnityId);
      
      boolean first = true;
      for (Map<String, String> tallRow : tallRows) {

        String variableId = tallRow.get(VARIABLE_ID_COL_NAME);
        
        validateTallRow(entity, tallRow, tallRowEnityId, errPrefix, variableId);
        
        String value = tallRow.get(VARIABLE_VALUE_COL_NAME);
        wideRow.put(variableId, value);

        // if first row, add ancestor PKs to wide table
        for (String ancestorPkColName : entity.getAncestorPkColNames()) {
          if (!tallRow.containsKey(ancestorPkColName))
            throw new RuntimeException(errPrefix + " does not contain column " + ancestorPkColName);
          if (first) wideRow.put(ancestorPkColName, tallRow.get(ancestorPkColName));
        }
        first = false;
      }
      return wideRow;
    };
  }
  
  private static void validateTallRow(Entity entity, Map<String,String> tallRow, String errPrefix, String tallRowEnityId, String variableId) {
    // do some simple validation
    if (tallRow.size() != entity.getTallRowSize()) 
      throw new RuntimeException(errPrefix + " has an unexpected number of columns: " + tallRow.size());
    
    if (!tallRow.get(entity.getPKColName()).equals(tallRowEnityId))
      throw new RuntimeException(errPrefix + " has an unexpected PK value");

    if (!tallRow.containsKey(VARIABLE_ID_COL_NAME) )
      throw new RuntimeException(errPrefix + " does not contain column " + VARIABLE_ID_COL_NAME);

    entity.getVariable(variableId)
        .orElseThrow(() -> new RuntimeException(errPrefix + " has an invalid variableId: " + variableId));
  }
  
}
