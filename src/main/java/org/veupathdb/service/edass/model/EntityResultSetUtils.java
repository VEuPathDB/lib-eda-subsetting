package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;

import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;

public class EntityResultSetUtils {

  public static final String VARIABLE_ID_COL_NAME = "variable_id";
  public static final String VARIABLE_VALUE_COL_NAME = "value";
    
  public static final String ENTITY_NAME_COL_NAME = "name";
  public static final String ENTITY_ID_COL_NAME = "entity_id";
  public static final String STUDY_ID_COL_NAME = "study_id";
  public static final String PARENT_ID_COL_NAME = "parent_id";
  public static final String DESCRIP_COL_NAME = "description";

  public static TreeNode<Entity> getStudyEntityTree(DataSource datasource, String studyId) {
    
    String sql = generateEntityTreeSql(studyId);
    
    Map<String, List<Entity>> simpleTree = new HashMap<String, List<Entity>>(); // entityID -> child entities
       
    Entity rootEntity = new SQLRunner(datasource, sql).executeQuery(rs -> {
      Entity root = null;
      while (rs.next()) {
        Entity entity = createEntityFromResultSet(rs);
        String parentId = rs.getString(PARENT_ID_COL_NAME);
        if (parentId == null) {
          if (root != null) throw new InternalServerErrorException("In Study " + studyId + " found more than one root entity");
          root = entity;
        }
        if (!simpleTree.containsKey(parentId)) simpleTree.put(parentId, new ArrayList<Entity>());
        simpleTree.get(parentId).add(entity);
      }
      return root;
    });
    
    if (rootEntity == null) throw new InternalServerErrorException("Found no entities for study: " + studyId);
    
    TreeNode<Entity> rootNode = new TreeNode<Entity>(rootEntity);
    populateEntityTree(rootNode, simpleTree.get(rootEntity.getId()), simpleTree);
    return rootNode;
  }
  
  static void populateEntityTree(TreeNode<Entity> parentNode, List<Entity> children, Map<String, List<Entity>> simpleTree) {
    for (Entity child : children) {
      TreeNode<Entity> childNode = new TreeNode<Entity>(child); 
      parentNode.addChildNode(childNode);
      populateEntityTree(childNode, simpleTree.get(child.getId()), simpleTree);
    }
  }
  
  private static String generateEntityTreeSql(String studyId) {
    return null;
    //TODO
  }

  private static Entity createEntityFromResultSet(ResultSet rs) {

    String studyId;
    try {
      studyId = rs.getString(STUDY_ID_COL_NAME);
      String name = rs.getString(ENTITY_NAME_COL_NAME);
      String id = rs.getString(ENTITY_ID_COL_NAME);
      String descrip = rs.getString(DESCRIP_COL_NAME);
      
      return new Entity(name, id, studyId + "_" + name + "_tall", descrip, studyId + "_" + name + "_ancestors", name + "_id");
    }
    catch (SQLException e) {
      throw new InternalServerErrorException(e);
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
          .orElseThrow(() -> new InternalServerErrorException("Can't find column in tall table result set: " + VARIABLE_ID_COL_NAME));

      tallRow.put(VARIABLE_VALUE_COL_NAME, var.getVariableType().convertRowValueToStringValue(rs));
      
      return tallRow;
    }
    catch (SQLException e) {
      throw new InternalServerErrorException(e);
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
            throw new InternalServerErrorException(errPrefix + " does not contain column " + ancestorPkColName);
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
      throw new InternalServerErrorException(errPrefix + " has an unexpected number of columns: " + tallRow.size());
    
    if (!tallRow.get(entity.getPKColName()).equals(tallRowEnityId))
      throw new InternalServerErrorException(errPrefix + " has an unexpected PK value");

    if (!tallRow.containsKey(VARIABLE_ID_COL_NAME) )
      throw new InternalServerErrorException(errPrefix + " does not contain column " + VARIABLE_ID_COL_NAME);

    entity.getVariable(variableId)
        .orElseThrow(() -> new InternalServerErrorException(errPrefix + " has an invalid variableId: " + variableId));
  }
  
}