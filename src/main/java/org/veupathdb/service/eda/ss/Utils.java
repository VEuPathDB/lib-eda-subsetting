package org.veupathdb.service.eda.ss;

public class Utils {

  // TODO: remove once the client is fixed to not send in trailing 'Z'
  public static String standardizeLocalDateTime(String dateTimeString) {
    return (dateTimeString == null || !dateTimeString.endsWith("Z"))
        ? dateTimeString
        : dateTimeString.substring(0, dateTimeString.length() - 1);
  }
}
