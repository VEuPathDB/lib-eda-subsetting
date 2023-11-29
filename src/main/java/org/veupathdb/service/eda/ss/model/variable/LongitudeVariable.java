package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.Utils;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DoubleValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.EmptyBinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

import java.nio.charset.StandardCharsets;

public class LongitudeVariable extends VariableWithValues<Double> {
  private static final int BYTE_COUNT_FOR_INTEGER_DECIMAL_AND_EXP_CHAR = 3;

  public static class Properties {

    private final Long precision;

    public Properties(Long precision) {
      this.precision = precision;
    }
  }

  private final Properties _properties;

  public LongitudeVariable(Variable.Properties varProps, VariableWithValues.Properties valueProps, Properties properties) {
    super(varProps, valueProps);
    validateType(VariableType.LONGITUDE);
    _properties = properties;
  }

  // static version for use when we don't have an instance
  public static BinaryConverter<Double> getGenericBinaryConverter(BinaryProperties empty) {
    return new DoubleValueConverter();
  }

  @Override
  public BinaryProperties getBinaryProperties() {
    return new EmptyBinaryProperties();
  }

  // instance method that provides typed return value
  @Override
  public BinaryConverter<Double> getBinaryConverter() {
    return getGenericBinaryConverter(null);
  }

  @Override
  public BinaryConverter<String> getStringConverter() {
    return Utils.getFloatingPointUtf8Converter(180, getPrecision().intValue());
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
