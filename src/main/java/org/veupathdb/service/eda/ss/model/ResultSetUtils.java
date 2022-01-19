package org.veupathdb.service.eda.ss.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.gusdb.fgputil.json.JsonUtil;

public class ResultSetUtils {

  public static String getRsStringWithDefault(ResultSet rs, String colName, String defaultVal) throws SQLException {
    return Optional.ofNullable(rs.getString(colName)).orElse(defaultVal);
  }

  public static Long getRsIntegerWithDefault(ResultSet rs, String colName, Long defaultVal) throws SQLException {
    long val = rs.getLong(colName);
    return rs.wasNull() ? defaultVal : val;
  }

  public static String getRsString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getRsString(rs, columnName, requireNonNull, null);
  }

  // need BigDecimal to handle strings representing values in
  public static Long getIntegerFromString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getNumberFromString(rs, columnName, requireNonNull, "integer", val -> new BigDecimal(val).longValueExact());
  }

  public static Double getDoubleFromString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getNumberFromString(rs, columnName, requireNonNull, "floating-point", val -> new BigDecimal(val).doubleValue());
  }

  private static String getRsString(ResultSet rs, String columnName, boolean requireNonNull, String typeDisplay) throws SQLException {
    String value = rs.getString(columnName);
    if (value == null && requireNonNull) {
      String typeAnnot = typeDisplay == null ? "" : " (" + typeDisplay + ")";
      throw new RuntimeException("Column " + columnName + " returned a null value but is required" + typeAnnot + ".");
    }
    return value;
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

  /**
   * Parses a string containing a json array of strings into a List
   */
  public static List<String> parseJsonArrayOfString(ResultSet resultSet, String columnName) throws SQLException {
    String jsonArrayOfString = resultSet.getString(columnName);
    try {
      return jsonArrayOfString == null ? null
          : Arrays.asList(JsonUtil.Jackson.readValue(jsonArrayOfString, String[].class));
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("Value in column " + columnName + " can not be parsed into json array of strings: " + jsonArrayOfString);
    }
  }
}
