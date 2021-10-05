package org.veupathdb.service.eda.ss.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class ResultSetUtils {
  public static String getRsStringNotNull(ResultSet rs, String colName) throws SQLException {
    return Optional.ofNullable(rs.getString(colName)).orElseThrow(
        () -> new RuntimeException("Found a null for variable column: " + colName));
  }

  public static String getRsStringWithDefault(ResultSet rs, String colName, String defaultVal) throws SQLException {
    return Optional.ofNullable(rs.getString(colName)).orElse(defaultVal);
  }

  public static Long getRsIntegerWithDefault(ResultSet rs, String colName, Long defaultVal) throws SQLException {
    long val = rs.getLong(colName);
    return rs.wasNull() ? defaultVal : val;
  }

  public static Long getIntegerFromString(String value) {
    return value == null ? null : Long.parseLong(value);
  }

  public static Double getDoubleFromString(String value) {
    return value == null ? null : Double.parseDouble(value);
  }
}
