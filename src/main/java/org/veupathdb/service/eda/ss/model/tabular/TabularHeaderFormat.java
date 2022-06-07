package org.veupathdb.service.eda.ss.model.tabular;

public enum TabularHeaderFormat {
  STANDARD("standard"),
  DISPLAY("display");

  private final String name;

  TabularHeaderFormat(String name) {
    this.name = name;
  }
  public String getValue(){ return name; }
}

