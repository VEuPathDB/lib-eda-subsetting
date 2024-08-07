package org.veupathdb.service.eda.subset.model.variable;

public enum VariableDisplayType {
  DEFAULT("default"),
  // FIXME: hidden is deprecated and should be removed
  //  from both this code and the RAML in EdaCommon
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

  public String getType() {
    return _type;
  }
}
