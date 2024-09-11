package org.veupathdb.service.eda.subset.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.subset.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.subset.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.EmptyBinaryProperties;
import org.veupathdb.service.eda.subset.model.variable.binary.LongValueConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.StringValueConverter;

import java.nio.charset.StandardCharsets;
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
  public BinaryConverter<String> getStringConverter() {
    return new StringValueConverter(_distributionConfig.getRangeMax().toString()
        .getBytes(StandardCharsets.UTF_8).length + Integer.BYTES);
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
  public Long getValidatedSubtype(Number value) {
    return getValidatedSubtype(value, "Passed value '" + value + "' must be an integer but is not.");
  }

  private static Long getValidatedSubtype(Number value, String errorMessage) {
    if (value.doubleValue() != Math.floor(value.doubleValue())) {
      throw new BadRequestException(errorMessage);
    }
    // passed number was an integer so truncation is fine here
    return value.longValue();
  }

  @Override
  public Long getValidatedSubtypeForInclusiveRangeBoundary(Number number, InclusiveRangeBoundary boundary) {
    Double d = number.doubleValue();
    // This conversion depends on range filters using inclusive boundaries!
    d = boundary == InclusiveRangeBoundary.MIN ? Math.ceil(d) : Math.floor(d);
    return d.longValue();
  }

  @Override
  public Long getValidatedSubtypeForBinWidth(Number binWidth) {
    String errorMessage = "binWidth must be a positive integer for integer variable distributions";
    long longValue = getValidatedSubtype(binWidth, errorMessage);
    if (longValue <= 0) {
      throw new BadRequestException(errorMessage);
    }
    return longValue;
  }

  public static Optional<IntegerVariable> assertType(Variable variable) {
    return Optional.ofNullable(variable instanceof IntegerVariable ? (IntegerVariable)variable : null);
  }
}
