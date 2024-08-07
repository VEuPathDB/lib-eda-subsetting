package org.veupathdb.service.eda.subset.model.variable;

public enum VariableScale {
  LOG_10("log"),
  LOG_2("log2"),
  NATURAL_LOG("ln");

  private final String _value;

  VariableScale(String value) {
    _value = value;
  }

  public static VariableScale findByValue(String value) {
    if (value == null || value.isBlank())
      return null;
    for (VariableScale scale : values()) {
      if (scale._value.equals(value))
        return scale;
    }
    throw new IllegalArgumentException("Invalid value for scale: " + value);
  }
}
