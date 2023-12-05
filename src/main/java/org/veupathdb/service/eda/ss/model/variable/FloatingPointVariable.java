package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DoubleValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class FloatingPointVariable extends NumberVariable<Double> {
  private static final int BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR = 2;
  private static final int MAX_DIGITS_BEFORE_SCIENTIFIC_NOTATION = 7;

  public static class Properties {

    public final String units;
    public final Long precision;
    public final VariableScale scale;

    public Properties(String units, Long precision, VariableScale scale) {
      this.units = units;
      this.precision = precision;
      this.scale = scale;
    }
  }

  private final Properties _properties;
  private final Utf8EncodingLengthProperties _binaryProperties;

  public FloatingPointVariable(
      Variable.Properties varProperties,
      VariableWithValues.Properties valueProperties,
      NumberDistributionConfig<Double> distributionConfig,
      Properties properties,
      Utf8EncodingLengthProperties binaryProperties) {

    super(varProperties, valueProperties, distributionConfig);
    _properties = properties;
    _binaryProperties = binaryProperties;
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
    return _binaryProperties;
  }

  @Override
  public BinaryConverter<Double> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    if (_binaryProperties != null) {
      return new StringValueConverter(_binaryProperties.getMaxLength());
    }
    // TODO: Remove this fallback once files are re-generated with new file dumper code.
    // Floating point values are greater than 1e7 are displayed in scientific notation. For this reason, the maximum
    // size of our string is our precision + 3 bytes for the integer part of the decimal, the "e" in scientific notation
    // and the integer part of our value. We also reserve 4 bytes for the size of the padded string.
    int integerPartBytes = Integer.toString(_distributionConfig.getRangeMax().intValue()).getBytes(StandardCharsets.UTF_8).length;
    int numBytesReservedForIntPart = Math.min(integerPartBytes, MAX_DIGITS_BEFORE_SCIENTIFIC_NOTATION); // After 7 digits, we start using scientific notation
    int bytesReservedForIntegerPartOrScientificNotation = Math.max(numBytesReservedForIntPart, BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR);
    return new StringValueConverter(Integer.BYTES // Reserved for all padded strings
        + bytesReservedForIntegerPartOrScientificNotation // Reserved for integer part and/or left part of scientific notation.
        + getPrecision().intValue() // Plus space for decimal part.
        + 1); // Plus one for decimal point.
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

  public VariableScale getScale() {
    return _properties.scale;
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
