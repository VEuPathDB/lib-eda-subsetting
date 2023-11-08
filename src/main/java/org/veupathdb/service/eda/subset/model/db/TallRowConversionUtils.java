package org.veupathdb.service.eda.subset.model.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.Variable;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;

/**
 * utilities for converting tall row SQL results into wide rows for tabular responses
 *
 * @author Steve
 */
public class TallRowConversionUtils {
  private static final Logger LOG = LogManager.getLogger(TallRowConversionUtils.class);

  static final String VARIABLE_VALUE_COL_NAME = "value";

  /**
   * Tall table rows look like this:
   * ancestor1_pk, ancestor2_pk, pk, variableA_id, string_value, number_value, date_value
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
      String variableId = rs.getString(DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME);
      tallRow.put(DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME, variableId);

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
   * <p>
   * Tall table rows look like this:
   * ancestor1_pk, ancestor2_pk, pk, variableA_id, value
   * ancestor1_pk, ancestor2_pk, pk, variableB_id, value
   * ancestor1_pk, ancestor2_pk, pk, variableC_id, value
   * <p>
   * (ordered by the ancestry IDs and then the variable ID)
   * <p>
   * Output wide row looks like this:
   * ancestor1_pk, ancestor2_pk, pk, variableA_value, variableB_value, variableC_value
   * <p>
   * (all values are converted to strings)
   */
  static Function<List<Map<String, String>>, Map<String, String>> getTallToWideFunction(Entity entity) {
    return getTallToWideFunction(entity, null, Entity::getPKColName, Variable::getId);
  }

  static Function<List<Map<String, String>>, Map<String, String>> getTallToWideFunction(Entity entity,
                                                                                        List<String> outputCols,
                                                                                        Function<Entity, String> entityKeyNameFn,
                                                                                        Function<Variable, String> variableKeyNameFn) {

    String errPrefix = "Tall row supplied to entity " + entity.getId();

    return tallRows -> {

      Map<String, String> firstTallRow = tallRows.get(0);
      Map<String, String> wideRow = new HashMap<>();
      String tallRowEntityId = firstTallRow.get(entity.getPKColName());
      Map<String, List<String>> multiValues = null;  // this map only contains variables that have multiple values
      Map<String, VariableWithValues> variablesMap = new HashMap<>(); // ID -> VariableWithValues

      addPrimaryKeysToWideRow(firstTallRow, entity, wideRow, tallRowEntityId, errPrefix, entityKeyNameFn);

      // loop through all tall rows and add vars to wide row, validating along the way
      // temporarily store any multi-values in a dedicated map.  (at the end, reduce them to JSON string)
      for (Map<String, String> tallRow : tallRows) {
        String variableId = tallRow.get(DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME);
        if (variableId != null) {

          // validate row, and add this var to variablesMap if not already there
          validateTallRow(entity, tallRow, errPrefix, tallRowEntityId, variableId, variablesMap);

          // handle multi-valued variable.
          // (this is a rare case, so only allocate the array if needed)
          if (variablesMap.get(variableId).getIsMultiValued()) {
            multiValues = updateMultiValuesMap(variableKeyNameFn.apply(variablesMap.get(variableId)), tallRow, multiValues);
          }

          // else handle single valued variable
          else {
            if (wideRow.containsKey(variableId)) throw new RuntimeException("Variable found to incorrectly have multiple values: " + variableId);
            wideRow.put(variableKeyNameFn.apply(variablesMap.get(variableId)), tallRow.get(VARIABLE_VALUE_COL_NAME));
          }
        }
      }

      // reduce multi-values to JSON string, and stuff into wide row
      if (multiValues != null) putMultiValuesAsJsonIntoWideRow(multiValues, wideRow, variablesMap);

      if (outputCols != null) {
        outputCols.forEach(col -> wideRow.putIfAbsent(col, ""));
      }

      return wideRow;
    };
  }

  private static void addPrimaryKeysToWideRow(Map<String, String> firstTallRow,
                                              Entity entity, Map<String, String> wideRow,
                                              String tallRowEntityId, String errPrefix, Function<Entity, String> idGetter) {
    // add entity PK to the wide row
    wideRow.put(entity.getId() + "." + entity.getPKColName(), tallRowEntityId);

    // add ancestor PKs to wide row
    for (Entity ancestorEntity : entity.getAncestorEntities()) {
      if (!firstTallRow.containsKey(ancestorEntity.getPKColName()))
        throw new RuntimeException(errPrefix + " does not contain column " + ancestorEntity.getPKColName());
      wideRow.put(idGetter.apply(ancestorEntity), firstTallRow.get(ancestorEntity.getPKColName()));
    }
  }

  // update provided map (side-effect) with a new multi-value for the given variable ID.
  // if map passed in is null, create new map
  private static Map<String, List<String>> updateMultiValuesMap(String variableId, Map<String, String> tallRow, Map<String, List<String>> multiValuesMap) {

    // if this is our first multi-valued guy, create the empty map
    if (multiValuesMap == null) multiValuesMap = new HashMap<>();

    String tallRowValue = tallRow.get(VARIABLE_VALUE_COL_NAME);

    // we either get a single tall row with a null, if the data is missing for this entity
    // or one or more rows with values.
    if (tallRowValue == null) {
      if (multiValuesMap.containsKey(variableId)) throw new RuntimeException("Unexpected null value for multi-valued variable: " + variableId);
      multiValuesMap.put(variableId, null);
    }
    else {
      if (!multiValuesMap.containsKey(variableId)) multiValuesMap.put(variableId, new ArrayList<>());
      multiValuesMap.get(variableId).add(tallRowValue);
    }
    return multiValuesMap;
  }

  // for those variables that have multi-values, transform the list of string values to 
  // appropriate json array, and put that array into the wide row
  protected static void putMultiValuesAsJsonIntoWideRow(Map<String, List<String>> multiValues,
                                                        Map<String, String> wideRow,
                                                        Map<String, VariableWithValues> variablesMap) {

    for (String multiValVarId : multiValues.keySet()) {
      VariableWithValues vwv = variablesMap.get(multiValVarId);
      JSONArray jsonArray = vwv.getType().convertStringListToJsonArray(multiValues.get(multiValVarId));
      wideRow.put(multiValVarId, jsonArray.toString());
    }
  }

  private static void validateTallRow(Entity entity, Map<String, String> tallRow, String errPrefix,
                                      String tallRowEnityId, String variableId, Map<String, VariableWithValues> variablesMap) {
    // do some simple validation
    if (tallRow.size() != entity.getTallRowSize())
      throw new RuntimeException(errPrefix + " has an unexpected number of columns: (" + tallRow.size() + "): " + tallRow.keySet());

    if (!tallRow.get(entity.getPKColName()).equals(tallRowEnityId))
      throw new RuntimeException(errPrefix + " has an unexpected PK value");

    if (!tallRow.containsKey(DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME))
      throw new RuntimeException(errPrefix + " does not contain column " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME);

    if (!variablesMap.containsKey(variableId)) {
      Variable var = entity.getVariable(variableId)
          .orElseThrow(() -> new RuntimeException(errPrefix + " has an invalid variableId: " + variableId));

      if (!(var instanceof VariableWithValues))
        throw new RuntimeException("Variable in tall result does not have values: " + variableId);

      variablesMap.put(variableId, (VariableWithValues)var);
    }
  }

}
