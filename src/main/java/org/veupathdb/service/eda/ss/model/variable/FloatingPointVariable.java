package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DoubleValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.EmptyBinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class FloatingPointVariable extends NumberVariable<Double> {
  private static final int BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR = 3;
  private static final int MAX_DIGITS_BEFORE_SCIENTIFIC_NOTATION = 7;

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

  public static BinaryConverter<Double> getGenericBinaryConverter(BinaryProperties empty) {
    return new DoubleValueConverter();
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return new EmptyBinaryProperties();
  }

  @Override
  public BinaryConverter<Double> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    // Floating point values are greater than 1e7 are displayed in scientific notation. For this reason, the maximum
    // size of our string is our precision + 3 bytes for the integer part of the decimal, the "e" in scientific notation
    // and the integer part of our value. We also reserve 4 bytes for the size of the padded string.
    int integerPartBytes = Integer.toString(_distributionConfig.getRangeMax().intValue()).getBytes(StandardCharsets.UTF_8).length;
    int numBytesReservedForIntPart = Math.min(integerPartBytes, 7); // After 7 digits, we start using scientific notation
    int bytesReservedForIntegerPartOrScienitificNotation = Math.max(numBytesReservedForIntPart, BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR);
    return new StringValueConverter(Integer.BYTES // Reserved for all padded strings
        + bytesReservedForIntegerPartOrScienitificNotation // Reserved for integer part o
        + getPrecision().intValue());
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
