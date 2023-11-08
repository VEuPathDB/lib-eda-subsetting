package org.veupathdb.service.eda.subset.model.tabular;

public enum TabularHeaderFormat {
  STANDARD("standard"),
  DISPLAY("display");

  private final String name;

  TabularHeaderFormat(String name) {
    this.name = name;
  }
  public String getValue(){ return name; }
}

