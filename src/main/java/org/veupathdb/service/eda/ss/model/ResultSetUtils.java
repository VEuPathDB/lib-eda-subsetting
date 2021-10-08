package org.veupathdb.service.eda.ss.model;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

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

  public static Long getIntegerFromString(ResultSet rs, String columnName, boolean requireNonNull) throws SQLException {
    return getNumberFromString(rs, columnName, requireNonNull, "integer", val -> new BigDecimal(val).longValue());
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
    catch (NumberFormatException e) {
      throw new RuntimeException(variableId + ": Column " + columnName + " returned a value not convertible to " + typeDisplay);
    }
  }
}
