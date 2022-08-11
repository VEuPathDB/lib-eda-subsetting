package org.veupathdb.service.eda.ss.model.tabular;

import java.util.Arrays;

public enum DataSourceType {
  FILES("file"),
  DATABASE("database");

  private String value;

  DataSourceType(String value) {
    this.value = value;
  }

  public static DataSourceType fromValue(String s) {
    return Arrays.stream(values())
        .filter(enumInstance -> enumInstance.value.equals(s))
        .findFirst()
        .orElseThrow();
  }
}
