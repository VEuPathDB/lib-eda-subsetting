package org.veupathdb.service.eda.ss.model.variable;

public enum VariableDisplayType {
  DEFAULT("default"),
  HIDDEN("hidden"),
  MULTIFILTER("multifilter"),
  GEOAGGREGATOR("geoaggregator"),
  LATITUDE("latitude"),
  LONGITUDE("longitude");

  private final String _type;

  VariableDisplayType(String type) {
    _type = type;
  }

  public static VariableDisplayType fromString(String displayType) {
    for (VariableDisplayType type : values()) {
      if (type._type.equals(displayType.toLowerCase())) {
        return type;
      }
    }
    throw new RuntimeException("Unrecognized variable display type: " + displayType);
  }
}
