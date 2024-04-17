package org.veupathdb.service.eda.subset.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.subset.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.DoubleValueConverter;
import org.veupathdb.service.eda.subset.model.variable.binary.StringValueConverter;

public class LongitudeVariable extends VariableWithValues<Double> {
  private static final int BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR = 3;

  public static class Properties {

    private final Long precision;

    public Properties(Long precision) {
      this.precision = precision;
    }
  }

  private final Properties _properties;
  private final Utf8EncodingLengthProperties _binaryProperties;

  public LongitudeVariable(Variable.Properties varProps, VariableWithValues.Properties valueProps, Properties properties, Utf8EncodingLengthProperties binaryProperties) {
    super(varProps, valueProps);
    validateType(VariableType.LONGITUDE);
    _properties = properties;
    _binaryProperties = binaryProperties;
  }

  // static version for use when we don't have an instance
  public static BinaryConverter<Double> getGenericBinaryConverter(BinaryProperties empty) {
    return new DoubleValueConverter();
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return _binaryProperties;
  }

  // instance method that provides typed return value
  @Override
  public BinaryConverter<Double> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    if (_binaryProperties.getMaxLength() != null) {
      return new StringValueConverter(_binaryProperties.getMaxLength());
    }
    // TODO: Remove this fallback once files are re-generated with new file dumper code.
    // Floating point values are greater than 1e7 are displayed in scientific notation. For this reason, the maximum
    // size of our string is our precision + 3 bytes for the integer part of the decimal, the "e" in scientific notation
    // and the integer part of our value. We also reserve 4 bytes for the size of the padded string.
    return new StringValueConverter(Integer.BYTES + BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR + getPrecision().intValue());
  }

  @Override
  public Double fromString(String s) {
    return Double.valueOf(s);
  }

  @Override
  public String valueToString(Double val, TabularReportConfig reportConfig) {
    return Double.toString(val);
  }

  public Long getPrecision() {
    return _properties.precision;
  }

  public static LongitudeVariable assertType(Variable variable) {
    if (variable instanceof LongitudeVariable) return (LongitudeVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a longitude variable.");
  }
}
