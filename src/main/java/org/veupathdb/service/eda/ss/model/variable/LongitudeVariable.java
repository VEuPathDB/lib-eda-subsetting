package org.veupathdb.service.eda.ss.model.variable;

public class LongitudeVariable extends VariableWithValues {

  public static class Properties {

    private final Integer precision;

    public Properties(Integer precision) {
      this.precision = precision;
    }
  }

  private final Properties _properties;

  public LongitudeVariable(Variable.Properties varProps, VariableWithValues.Properties valueProps, Properties properties) {
    super(varProps, valueProps);
    validateType(VariableType.LONGITUDE);
    _properties = properties;
  }

  public Integer getPrecision() {
    return _properties.precision;
  }
}
