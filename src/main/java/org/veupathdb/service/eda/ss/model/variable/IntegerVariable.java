package org.veupathdb.service.eda.ss.model.variable;

public class IntegerVariable extends VariableWithValues implements SupportsNumericRange<Integer> {

  public static class Properties {

    public final String units;
    public final Integer displayRangeMin;
    public final Integer displayRangeMax;
    public final Integer rangeMin;
    public final Integer rangeMax;
    public final Integer binWidth;
    public final Integer binWidthOverride;

    public Properties(String units, Integer displayRangeMin, Integer displayRangeMax,
                      Integer rangeMin, Integer rangeMax,
                      Integer binWidth, Integer binWidthOverride) {
      this.units = units;
      this.displayRangeMin = displayRangeMin;
      this.displayRangeMax = displayRangeMax;
      this.rangeMin = rangeMin;
      this.rangeMax = rangeMax;
      this.binWidthOverride = binWidthOverride;
      this.binWidth = binWidth;
    }
  }

  private final Properties _properties;

  public IntegerVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, Properties properties) {

    super(varProperties, valueProperties);
    _properties = properties;
    validateType(VariableType.INTEGER);

    String errPrefix = "In entity " + varProperties.entity.getId() + " variable " + varProperties.id + " has a null ";
    if (_properties.units == null)
      throw new RuntimeException(errPrefix + "units");

  }

  public String getUnits() {
    return _properties.units;
  }

  @Override
  public Integer getDisplayRangeMin() {
    return _properties.displayRangeMin;
  }

  @Override
  public Integer getDisplayRangeMax() {
    return _properties.displayRangeMax;
  }

  @Override
  public Integer getRangeMin() {
    return _properties.rangeMin;
  }

  @Override
  public Integer getRangeMax() {
    return _properties.rangeMax;
  }

  @Override
  public Integer getBinWidthOverride() {
    return _properties.binWidthOverride;
  }

  @Override
  public Integer getBinWidth() {
    return _properties.binWidth;
  }

  @Override
  public Integer getDefaultBinWidth() {
    return _properties.binWidthOverride == null
        ? _properties.binWidth
        : _properties.binWidthOverride;
  }
}
