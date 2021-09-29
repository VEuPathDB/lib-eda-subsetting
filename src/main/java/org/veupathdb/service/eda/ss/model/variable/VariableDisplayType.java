package org.veupathdb.service.eda.ss.model.variable;

public enum VariableDisplayType {
  DEFAULT("default"),
  HIDDEN("hidden"),
  MULTIFILTER("multiFilter");

  String type;

  VariableDisplayType(String type) {
    this.type = type;
  }

  public static VariableDisplayType fromString(String displayType) {

    VariableDisplayType t;

    switch (displayType.toLowerCase()) {
      case "default" -> t = DEFAULT;
      case "multifilter" -> t = MULTIFILTER;
      case "hidden" -> t = HIDDEN;
      default -> throw new RuntimeException("Unrecognized variable display type: " + displayType);
    }
    return t;
  }

  public String getType() {return type;}
}
