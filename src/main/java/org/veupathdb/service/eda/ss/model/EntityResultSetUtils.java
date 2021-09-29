package org.veupathdb.service.eda.ss.model;

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
import org.json.JSONArray;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.variable.Variable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.RdbmsColumnNames.*;
import static org.veupathdb.service.eda.ss.model.ResultSetUtils.getRsStringNotNull;
import static org.veupathdb.service.eda.ss.model.ResultSetUtils.getRsStringWithDefault;

/**
 * utilities for creating Entities from result sets
 * @author Steve
 *
 */
public class EntityResultSetUtils {

  private final static String STDY_ABBRV_COL_NM = "study_abbrev"; // for private queries

  static TreeNode<Entity> getStudyEntityTree(DataSource datasource, String studyId) {
    
    String sql = generateEntityTreeSql(studyId);
    
    // entityID -> list of child entities
    Map<String, List<Entity>> simpleTree = new HashMap<>();
    
    Entity rootEntity = new SQLRunner(datasource, sql, "Get entity tree").executeQuery(rs -> {
      Entity root = null;
      while (rs.next()) {
        Entity entity = createEntityFromResultSet(rs);
        String parentId = rs.getString(ENTITY_PARENT_ID_COL_NAME);
        if (parentId == null) {
          if (root != null) throw new RuntimeException("In Study " + studyId + " found more than one root entity");
          root = entity;
        } else {
          if (!simpleTree.containsKey(parentId)) simpleTree.put(parentId, new ArrayList<>());
          simpleTree.get(parentId).add(entity);
        }
      }
      return root;
    });
    
    if (rootEntity == null) throw new RuntimeException("Found no entities for study: " + studyId);

    List<Entity> rootKids = simpleTree.get(rootEntity.getId());
    TreeNode<Entity> rootNode = new TreeNode<>(rootEntity);
    populateEntityTree(rootNode, rootKids, simpleTree);
    return rootNode;
  }
  
  static void populateEntityTree(TreeNode<Entity> parentNode, List<Entity> children, Map<String, List<Entity>> simpleTree) {
    for (Entity child : children) {
      TreeNode<Entity> childNode = new TreeNode<>(child);
      parentNode.addChildNode(childNode);
      
      // if this node has children, recurse
      if (simpleTree.containsKey(child.getId()))
        populateEntityTree(childNode, simpleTree.get(child.getId()), simpleTree);
    }
  }
  
  static String generateEntityTreeSql(String studyId) {
    String[] entityCols = {STUDY_ID_COL_NAME, ENTITY_ABBREV_COL_NAME, DISPLAY_NAME_COL_NAME, DISPLAY_NAME_PLURAL_COL_NAME, ENTITY_ID_COL_NAME, DESCRIP_COL_NAME, ENTITY_PARENT_ID_COL_NAME};
    return "SELECT e." + String.join(", e.", entityCols) + ", s." + STUDY_ABBREV_COL_NAME + " as " + STDY_ABBRV_COL_NM + NL
        + "FROM " + Resources.getAppDbSchema() + ENTITY_TABLE_NAME + " e," + Resources.getAppDbSchema() + STUDY_TABLE_NAME + " s " + NL
        + "WHERE s." + STUDY_ID_COL_NAME + " = '" + studyId + "'" + NL
        + "AND e." + ENTITY_STUDY_ID_COL_NAME + " = s." + STUDY_ID_COL_NAME  + NL
        + "ORDER BY e." + ENTITY_ID_COL_NAME;  // stable ordering supports unit testing
  }

  static Entity createEntityFromResultSet(ResultSet rs) {

    try {
      String name = getRsStringNotNull(rs, DISPLAY_NAME_COL_NAME);
      String namePlural = rs.getString(DISPLAY_NAME_PLURAL_COL_NAME);
      if (namePlural == null) namePlural = name + "s";  // TODO remove this hack when db has plurals
      String id = getRsStringNotNull(rs, ENTITY_ID_COL_NAME);
      String studyAbbrev = getRsStringNotNull(rs, STDY_ABBRV_COL_NM);
      String descrip = getRsStringWithDefault(rs, DESCRIP_COL_NAME, "No Entity Description available");
      String abbrev = getRsStringNotNull(rs, ENTITY_ABBREV_COL_NAME);

      return new Entity(id, studyAbbrev, name, namePlural, descrip, abbrev);
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Tall table rows look like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_id, string_value, number_value, date_value
   */
  static Map<String, String> resultSetToTallRowMap(Entity entity, ResultSet rs) {

    Map<String, String> tallRow = new HashMap<>();

    try {
      // add entity PK to map
      tallRow.put(entity.getPKColName(), rs.getString(entity.getPKColName()));
      // add ancestor PKs to map
      for (String colName : entity.getAncestorPkColNames()) {
        tallRow.put(colName, rs.getString(colName));
      }
      // add variable ID to map (may be null!)
      String variableId = rs.getString(TT_VARIABLE_ID_COL_NAME);
      tallRow.put(TT_VARIABLE_ID_COL_NAME, variableId);

      // add variable value to map (skip if ID is null)
      if (variableId != null) {
        VariableWithValues var = entity.getVariable(variableId)
          .map(v -> (VariableWithValues)v)
          .orElseThrow(() -> new RuntimeException(
              "Metadata does not have variable found in tall table result set: " + variableId));

        tallRow.put(VARIABLE_VALUE_COL_NAME, var.getType().convertRowValueToStringValue(rs));
      }
      return tallRow;
    }
    catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  /**
   * Return a function that transforms a list of tall table rows for a given entity ID to a single wide row.
   * 
   * Tall table rows look like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_id, value
   *   ancestor1_pk, ancestor2_pk, pk, variableB_id, value
   *   ancestor1_pk, ancestor2_pk, pk, variableC_id, value
   *   
   *   (ordered by the ancestry IDs and then the variable ID)
   *   
   * Output wide row looks like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_value, variableB_value, variableC_value
   *   
   *   (all values are converted to strings)
   */
  static Function<List<Map<String, String>>, Map<String, String>> getTallToWideFunction(Entity entity) {
    
    String errPrefix = "Tall row supplied to entity " + entity.getId();

    return tallRows -> {

      Map<String,String> firstTallRow = tallRows.get(0);
      Map<String, String> wideRow = new HashMap<>();
      String tallRowEntityId = firstTallRow.get(entity.getPKColName());
      Map<String, List<String>> multiValues = null;  // this map only contains variables that have multiple values
      Map<String, VariableWithValues> variablesMap = new HashMap<String, VariableWithValues>(); // ID -> VariableWithValues
      
      addPrimaryKeysToWideRow(firstTallRow, entity, wideRow, tallRowEntityId, errPrefix);

      // loop through all tall rows and add vars to wide row, validating along the way
      // temporarily store any multi-values in a dedicated map.  (at the end, reduce them to JSON string)
      for (Map<String, String> tallRow : tallRows) {
        String variableId = tallRow.get(TT_VARIABLE_ID_COL_NAME);
        if (variableId == null) continue;
        else {
          
          // validate row, and add this var to variablesMap if not already there
          validateTallRow(entity, tallRow, errPrefix, tallRowEntityId, variableId, variablesMap);
          
          // handle multi valued variable.
          // (this is a rare case, so only allocate the array if needed)
          if (variablesMap.get(variableId).getIsMultiValued()) multiValues = updateMultiValuesMap(variableId, tallRow, multiValues);
          
          // else handle single valued variable
          else {
            if (wideRow.containsKey(variableId)) throw new RuntimeException("Variable found to incorrectly have multiple values: " + variableId);
            wideRow.put(variableId, tallRow.get(VARIABLE_VALUE_COL_NAME));
          }
        }
      }
      
      // reduce multi-values to JSON string, and stuff into wide row
      if (multiValues != null) putMultiValuesAsJsonIntoWideRow(multiValues, wideRow, entity, variablesMap);
      
      return wideRow;
    };
  }

  private static void addPrimaryKeysToWideRow(Map<String,String> firstTallRow, 
      Entity entity, Map<String, String> wideRow, String tallRowEntityId, String errPrefix) {
    // add entity PK to the wide row
     wideRow.put(entity.getPKColName(), tallRowEntityId);

    // add ancestor PKs to wide row
    for (String ancestorPkColName : entity.getAncestorPkColNames()) {
      if (!firstTallRow.containsKey(ancestorPkColName))
        throw new RuntimeException(errPrefix + " does not contain column " + ancestorPkColName);
      wideRow.put(ancestorPkColName, firstTallRow.get(ancestorPkColName));
    }
  }
  
  // update provided map (side-effect) with a new multi-value for the given variable ID.
  // if map passed in is null, create new map
  private static Map<String, List<String>> updateMultiValuesMap(String variableId, Map<String, String> tallRow, Map<String, List<String>> multiValuesMap) {

    // if this is our first multi-valued guy, create the empty map
    if (multiValuesMap == null)  multiValuesMap = new HashMap<String, List<String>>();

    String tallRowValue = tallRow.get(VARIABLE_VALUE_COL_NAME);
	  
    // we either get a single tall row with a null, if the data is missing for this entity
    // or one or more rows with values.
    if (tallRowValue == null) {
      if (multiValuesMap.containsKey(variableId)) throw new RuntimeException("Unexpected null value for multi-valued variable: " + variableId);
      multiValuesMap.put(variableId, null);
    }
    else {
      if (!multiValuesMap.containsKey(variableId)) multiValuesMap.put(variableId, new ArrayList<String>());
      multiValuesMap.get(variableId).add(tallRowValue);
    }
	  return multiValuesMap;
  }
  
  // for those variables that have multi-values, transform the list of string values to 
  // appropriate json array, and put that array into the wide row
	protected static void putMultiValuesAsJsonIntoWideRow(Map<String, List<String>> multiValues,
			Map<String, String> wideRow, Entity entity, Map<String, VariableWithValues> variablesMap) {
	  
		for (String multiValVarId : multiValues.keySet()) {
			VariableWithValues vwv = variablesMap.get(multiValVarId);
			JSONArray jsonArray = vwv.getType().convertStringListToJsonArray(multiValues.get(multiValVarId));
			wideRow.put(multiValVarId, jsonArray.toString());
		}
	}
  
  private static void validateTallRow(Entity entity, Map<String,String> tallRow, String errPrefix, 
      String tallRowEnityId, String variableId, Map<String, VariableWithValues> variablesMap) {
    // do some simple validation
    if (tallRow.size() != entity.getTallRowSize()) 
      throw new RuntimeException(errPrefix + " has an unexpected number of columns: (" + tallRow.size() + "): " + tallRow.keySet());
    
    if (!tallRow.get(entity.getPKColName()).equals(tallRowEnityId))
      throw new RuntimeException(errPrefix + " has an unexpected PK value");

    if (!tallRow.containsKey(TT_VARIABLE_ID_COL_NAME) )
      throw new RuntimeException(errPrefix + " does not contain column " + TT_VARIABLE_ID_COL_NAME);
    
    if (!variablesMap.containsKey(variableId)) {
      Variable var = entity.getVariable(variableId)
          .orElseThrow(() -> new RuntimeException(errPrefix + " has an invalid variableId: " + variableId));

      if (!(var instanceof VariableWithValues))
        throw new RuntimeException("Variable in tall result does not have values: " + variableId);

      variablesMap.put(variableId, (VariableWithValues) var);
    }
  }
  
}
