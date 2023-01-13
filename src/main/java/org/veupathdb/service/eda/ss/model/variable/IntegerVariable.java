package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.EmptyBinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;
import java.util.Optional;

public class IntegerVariable extends NumberVariable<Long> {

  public static class Properties {

    public final String units;

    public Properties(String units) {
      this.units = units;
    }
  }

  private final Properties _properties;

  public IntegerVariable(
      Variable.Properties varProperties,
      VariableWithValues.Properties valueProperties,
      NumberDistributionConfig<Long> distributionConfig,
      Properties properties) {

    super(varProperties, valueProperties, distributionConfig);
    _properties = properties;
    validateType(VariableType.INTEGER);

    String errPrefix = "In entity " + varProperties.entity.getId() + " variable " + varProperties.id + " has a null ";
    if (_properties.units == null)
      throw new RuntimeException(errPrefix + "units");

  }
  
  // static version for use when we don't have an instance
  public static BinaryConverter<Long> getGenericBinaryConverter(BinaryProperties empty) {
    return new LongValueConverter();
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return new EmptyBinaryProperties();
  }

  @Override
  public BinaryConverter<Long> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public Long fromString(String s) {
    return Long.valueOf(s);
  }

  @Override
  public String valueToString(Long val, TabularReportConfig reportConfig) {
    return Long.toString(val);
  }

  @Override
  public String getUnits() {
    return _properties.units;
  }

  @Override
  public Long toNumberSubtype(Number number) {
    return number.longValue();
  }

  @Override
  public Long validateBinWidth(Number binWidth) {
    long longValue = toNumberSubtype(binWidth);
    if (longValue <= 0) {
      throw new BadRequestException("binWidth must be a positive integer for integer variable distributions");
    }
    return longValue;
  }

  public static Optional<IntegerVariable> assertType(Variable variable) {
    return Optional.ofNullable(variable instanceof IntegerVariable ? (IntegerVariable)variable : null);
  }
}
