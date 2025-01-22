package org.veupathdb.service.eda.subset.model.db;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.gusdb.fgputil.json.JsonUtil;

import static org.gusdb.fgputil.functional.Functions.doThrow;

public class ResultSetUtils {

  /**************************************************************************************
   *** "Simple" methods that return values if present; otherwise, returns:
   ***    - default (if optional)
   ***    - exception (if required)
   **************************************************************************************/

  public static Long getRsOptionalLong(ResultSet rs, String columnName, Long defaultVal) throws SQLException {
    long val = rs.getLong(columnName);
    return rs.wasNull() ? defaultVal : Long.valueOf(val);
  }

  public static Long getRsRequiredLong(ResultSet rs, String columnName) throws SQLException {
    long val = rs.getLong(columnName);
    return !rs.wasNull() ? val : doThrow(() -> new RuntimeException("Column " + columnName + " is required but returned null."));
  }

  public static Boolean getRsRequiredBoolean(ResultSet rs, String columnName) throws SQLException {
    boolean val = rs.getBoolean(columnName);
    return !rs.wasNull() ? val : doThrow(() -> new RuntimeException(
        "Column " + columnName + " is required but returned null."));
  }

  public static Boolean getRsOptionalBoolean(ResultSet rs, String columnName, boolean defaultVal) throws SQLException {
    boolean val = rs.getBoolean(columnName);
    return rs.wasNull() ? defaultVal : val;
  }

  public static String getRsOptionalString(ResultSet rs, String columnName, String defaultVal) throws SQLException {
    return Optional.ofNullable(rs.getString(columnName)).orElse(defaultVal);
  }

  public static String getRsRequiredString(ResultSet rs, String columnName) throws SQLException {
    return getRsString(rs, columnName, true, null);
  }

  /**************************************************************************************
   *** "Specialty" methods that read a string value for a column in a row that must also
   *** contain a "stable_id" column.  The string value is then converted to a number and
   *** returned.  If value is optional and original string value was null, null is returned.
   **************************************************************************************/

  // need BigDecimal to handle strings representing values in
  public static Long getIntegerFromString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getNumberFromString(rs, columnName, requireNonNull, "integer", val -> new BigDecimal(val).longValueExact());
  }

  public static Double getDoubleFromString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getNumberFromString(rs, columnName, requireNonNull, "floating-point", val -> new BigDecimal(val).doubleValue());
  }

  private static <T extends Number> T getNumberFromString(ResultSet rs, String columnName,
      boolean requireNonNull, String typeDisplay, Function<String,T> converter) throws SQLException {
    String value = getRsString(rs, columnName, requireNonNull, typeDisplay);
    String variableId = getRsString(rs, "stable_id", true, null);
    try {
      return value == null ? null : converter.apply(value);
    }
    catch (ArithmeticException e) {
      throw new RuntimeException("For variable '" + variableId + "', the metadata property '" + columnName + "' returned '" + value +
                                 "' which is not convertible to this variable's datatype, which is '" + typeDisplay + "'");
    }
  }

  private static String getRsString(ResultSet rs, String columnName, boolean requireNonNull, String typeDisplay) throws SQLException {
    String value = rs.getString(columnName);
    if (rs.wasNull() && requireNonNull) {
      String typeAnnot = typeDisplay == null ? "" : " (" + typeDisplay + ")";
      throw new RuntimeException("Column " + columnName + " returned a null value but is required" + typeAnnot + ".");
    }
    return value;
  }

  /**
   * Parses a string containing a json array of strings into a List
   */
  public static List<String> parseJsonArrayOfString(ResultSet resultSet, String columnName) throws SQLException {
    String jsonArrayOfString = resultSet.getString(columnName);
    try {
      return jsonArrayOfString == null || jsonArrayOfString.isEmpty() ? null
          : Arrays.asList(JsonUtil.Jackson.readValue(jsonArrayOfString, String[].class));
    }
    catch (IOException e) {
      throw new RuntimeException("Value in column " + jsonArrayOfString + " cannot be parsed into json array of strings: " + jsonArrayOfString, e);
    }
  }
}
