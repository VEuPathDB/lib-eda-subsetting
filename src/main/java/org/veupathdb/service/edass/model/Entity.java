/**
 * 
 */
package org.veupathdb.service.edass.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.ws.rs.InternalServerErrorException;

/**
 * @author Steve
 *
 */
public class Entity {
  private String id;
  private String name;
  private String tallTableName;
  private String ancestorsTableName;
  private String primaryKeyColumnName;
  private Map<String, Variable> variablesMap = new HashMap<String, Variable>();
  private List<Entity> ancestorEntities;
  private List<String> ancestorPkColNames;
  private List<String> ancestorFullPkColNames; // entityName.pkColName
  private Integer tallRowSize; // number of columns in a tall table row
  
  public static final String VARIABLE_ID_COL_NAME = "variable_id";
  public static final String VARIABLE_VALUE_COL_NAME = "value";
  
  public Entity(String entityName, String entityId, String entityTallTableName, String entityAncestorsTableName,
      String entityPrimaryKeyColumnName) {
    this.id = entityId;
    this.name = entityName;
    this.tallTableName = entityTallTableName;
    this.ancestorsTableName = entityAncestorsTableName;
    this.primaryKeyColumnName = entityPrimaryKeyColumnName;
  }

  public String getId() {
    return id;
  }

  String getName() {
    return name;
  }

  public String getTallTableName() {
    return tallTableName;
  }

  public String getPKColName() {
    return primaryKeyColumnName;
  }
  
  public String getFullPKColName() {
    return name + "." + primaryKeyColumnName;
  }
  
  public String getParentTableName() {
    return ancestorsTableName;
  }
  
  public List<String> getAncestorPkColNames() {
    return Collections.unmodifiableList(ancestorPkColNames);
  }
  
  public List<String> getAncestorFullPkColNames() {
    return Collections.unmodifiableList(ancestorFullPkColNames);
  }
  
  public void setAncestorEntities(List<Entity> ancestorEntities) {
    this.ancestorEntities = new ArrayList<Entity>(ancestorEntities);
    this.ancestorPkColNames = 
        ancestorEntities.stream().map(entry -> entry.getPKColName()).collect(Collectors.toList());
    this.ancestorFullPkColNames = 
        ancestorEntities.stream().map(entry -> entry.getName() + "." + entry.getPKColName()).collect(Collectors.toList());
  }

  public List<Entity> getAncestorEntities() {
    return Collections.unmodifiableList(ancestorEntities);
  }
  
  public String getEntityAncestorsTableName() {
    return ancestorsTableName;
  }

  public String toString() {
    return "id: " + getId() + " name: " + getName() + " (" + super.toString() + ")";
  }
  
  public String getAllPksSelectList(String entityTableName, String ancestorTableName) {
    List<String> selectColsList = new ArrayList<String>();
    for (String name : getAncestorPkColNames()) selectColsList.add(ancestorTableName + "." + name);
  
    selectColsList.add(entityTableName + "." + getPKColName());
    return String.join(", ", selectColsList);
  }
  
  /**
   * Tall table rows look like this:
   *   ancestor1_pk, ancestor2_pk, pk, variableA_id, string_value, number_value, date_value

   * @param rs
   * @return
   */
  public Map<String, String> resultSetToTallRowMap(ResultSet rs, List<String> olNames) {

    Map<String, String> tallRow = new HashMap<String, String>();

    try {
      for (String colName : getAncestorPkColNames()) {
        tallRow.put(colName, rs.getString(colName));
      }
      tallRow.put(getPKColName(), rs.getString(getPKColName()));
      tallRow.put(VARIABLE_ID_COL_NAME, rs.getString(VARIABLE_ID_COL_NAME));
      
      if (!variablesMap.containsKey(rs.getString(VARIABLE_ID_COL_NAME)))
          throw new InternalServerErrorException("Can't find column in tall table result set: " + VARIABLE_ID_COL_NAME);
      
      Variable var = variablesMap.get(rs.getString(VARIABLE_ID_COL_NAME));
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
  public Function<List<Map<String, String>>, Map<String, String>> getTallToWideFunction() {
    
    String errPrefix = "Tall row supplied to entity " + id;

    return tallRows -> {
      
      Map<String, String> wideRow = new HashMap<String, String>();

      String tallRowEnityId = tallRows.get(0).get(primaryKeyColumnName);
      wideRow.put(primaryKeyColumnName, tallRowEnityId);
      
      boolean first = true;
      for (Map<String, String> tallRow : tallRows) {

        String variableId = tallRow.get(VARIABLE_ID_COL_NAME);
        
        validateTallRow(tallRow, tallRowEnityId, errPrefix, variableId);
        
        String value = tallRow.get(VARIABLE_VALUE_COL_NAME);
        wideRow.put(variableId, value);

        // if first row, add ancestor PKs to wide table
        for (String ancestorPkColName : ancestorPkColNames) {
          if (!tallRow.containsKey(ancestorPkColName))
            throw new InternalServerErrorException(errPrefix + " does not contain column " + ancestorPkColName);
          if (first) wideRow.put(ancestorPkColName, tallRow.get(ancestorPkColName));
        }
        first = false;
      }
      return wideRow;
    };
  }
  
  private void validateTallRow(Map<String,String> tallRow, String errPrefix, String tallRowEnityId, String variableId) {
    // do some simple validation
    if (tallRow.size() != getTallRowSize()) 
      throw new InternalServerErrorException(errPrefix + " has an unexpected number of columns: " + tallRow.size());
    
    if (!tallRow.get(primaryKeyColumnName).equals(tallRowEnityId))
      throw new InternalServerErrorException(errPrefix + " has an unexpected PK value");

    if (!tallRow.containsKey(VARIABLE_ID_COL_NAME) )
      throw new InternalServerErrorException(errPrefix + " does not contain column " + VARIABLE_ID_COL_NAME);

    if (!variablesMap.containsKey(variableId))
      throw new InternalServerErrorException(errPrefix + " has an invalid variableId: " + variableId);
  }
  
  // ancestor PKs, pk, variable_id, int_value, str_value, date_value
  private Integer getTallRowSize() {
    if (tallRowSize == null) tallRowSize = Integer.valueOf(ancestorEntities.size() + 5);
    return tallRowSize;
  }

  public void addVariable(Variable var) {
    variablesMap.put(var.getId(), var);
  }
}
