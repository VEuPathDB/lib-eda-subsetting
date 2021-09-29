package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;
import org.veupathdb.service.eda.ss.model.Entity;

public class FloatingPointVariable extends VariableWithValues implements SupportsNumericRange<Double> {

  public static class Properties {

    public final String units;
    public final Integer precision;
    public final Double displayRangeMin;
    public final Double displayRangeMax;
    public final Double rangeMin;
    public final Double rangeMax;
    public final Double binWidth;
    public final Double binWidthOverride;

    public Properties(String units, Integer precision,
                      Double displayRangeMin, Double displayRangeMax,
                      Double rangeMin, Double rangeMax,
                      Double binWidth, Double binWidthOverride) {
      this.units = units;
      this.precision = precision;
      this.displayRangeMin = displayRangeMin;
      this.displayRangeMax = displayRangeMax;
      this.rangeMin = rangeMin;
      this.rangeMax = rangeMax;
      this.binWidthOverride = binWidthOverride;
      this.binWidth = binWidth;
    }
  }

  private final Properties _properties;

  public FloatingPointVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, Properties properties) {

    super(varProperties, valueProperties);
    _properties = properties;
    validateType(VariableType.NUMBER);

    String errPrefix = "In entity " + varProperties.entity.getId() + " variable " + varProperties.id + " has a null ";
    if (_properties.units == null)
      throw new RuntimeException(errPrefix + "units");
    if (_properties.precision == null)
      throw new RuntimeException(errPrefix + "precision");

  }

  public String getUnits() {
    return _properties.units;
  }

  public Integer getPrecision() {
    return _properties.precision;
  }

  @Override
  public Double getDisplayRangeMin() {
    return _properties.displayRangeMin;
  }

  @Override
  public Double getDisplayRangeMax() {
    return _properties.displayRangeMax;
  }

  @Override
  public Double getRangeMin() {
    return _properties.rangeMin;
  }

  @Override
  public Double getRangeMax() {
    return _properties.rangeMax;
  }

  @Override
  public Double getBinWidthOverride() {
    return _properties.binWidthOverride;
  }

  @Override
  public Double getBinWidth() {
    return _properties.binWidth;
  }

  @Override
  public Double getDefaultBinWidth() {
    return _properties.binWidthOverride == null
        ? _properties.binWidth
        : _properties.binWidthOverride;
  }
}
