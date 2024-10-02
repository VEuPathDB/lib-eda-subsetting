package org.veupathdb.service.eda.subset.model.tabular;

import java.util.Arrays;

public enum DataSourceType {
  FILES("file"),
  DATABASE("database"),
  UNSPECIFIED(null);

  private final String value;

  DataSourceType(String value) {
    this.value = value;
  }

  public static DataSourceType fromValue(String s) {
    if (s == null) {
      return UNSPECIFIED;
    }
    return Arrays.stream(values())
      .filter(enumInstance -> enumInstance.value.equals(s))
      .findFirst()
      .orElseThrow();
  }
}
