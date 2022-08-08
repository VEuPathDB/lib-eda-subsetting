package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DoubleValueConverter;

import java.util.Optional;

public class FloatingPointVariable extends NumberVariable<Double> {

  public static class Properties {

    public final String units;
    public final Long precision;

    public Properties(String units, Long precision) {
      this.units = units;
      this.precision = precision;
    }
  }

  private final Properties _properties;

  public FloatingPointVariable(
      Variable.Properties varProperties,
      VariableWithValues.Properties valueProperties,
      NumberDistributionConfig<Double> distributionConfig,
      Properties properties) {

    super(varProperties, valueProperties, distributionConfig);
    _properties = properties;
    validateType(VariableType.NUMBER);

    String errPrefix = "In entity " + varProperties.entity.getId() + " variable " + varProperties.id + " has a null ";
    if (_properties.units == null)
      throw new RuntimeException(errPrefix + "units");
    if (_properties.precision == null)
      throw new RuntimeException(errPrefix + "precision");

  }

  public static BinaryConverter<Double> getGenericBinaryConverter() {
    return new DoubleValueConverter();
  }

  @Override
  public BinaryConverter<Double> getBinaryConverter() {
    return getGenericBinaryConverter();
  }

  @Override
  public Double fromString(String s) {
    return Double.valueOf(s);
  }

  @Override
  public String valueToString(Double val, TabularReportConfig reportConfig) {
    return Double.toString(val);
  }

  @Override
  public String getUnits() {
    return _properties.units;
  }

  public Long getPrecision() {
    return _properties.precision;
  }

  @Override
  public Double toNumberSubtype(Number number) {
    return number.doubleValue();
  }

  @Override
  public Double validateBinWidth(Number binWidth) {
    double doubleValue = toNumberSubtype(binWidth);
    if (doubleValue <= 0) {
      throw new BadRequestException("binWidth must be a positive number for number variable distributions");
    }
    return doubleValue;
  }

  public static Optional<FloatingPointVariable> assertType(Variable variable) {
    return Optional.ofNullable(variable instanceof FloatingPointVariable ? (FloatingPointVariable)variable : null);
  }
}
