package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateVariable extends VariableWithValues<Long> {
  private final DateDistributionConfig _distributionConfig;

  public DateVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, DateDistributionConfig distributionConfig) {
    super(varProperties, valueProperties);
    _distributionConfig = distributionConfig;
    validateType(VariableType.DATE);
  }

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
    return new StringValueConverter(24);
  }

  @Override
  public Long fromString(String s) {
    return FormatUtil.parseDateTime(s).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Override
  public String valueToString(Long val, TabularReportConfig reportConfig) {
    if (reportConfig.getTrimTimeFromDateVars()) {
      return DateTimeFormatter.ISO_LOCAL_DATE.format(Instant.ofEpochMilli(val).atOffset(ZoneOffset.UTC));
    }
    return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(Instant.ofEpochMilli(val).atOffset(ZoneOffset.UTC));
  }

  @Override
  public String valueToJsonText(Long val, TabularReportConfig config) {
    return quote(valueToString(val, config));
  }

  @Override
  public byte[] valueToJsonTextBytes(Long val, TabularReportConfig config) {
    return quote(valueToString(val, config)).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] valueToUtf8Bytes(Long val, TabularReportConfig config) {
    return valueToString(val, config).getBytes(StandardCharsets.UTF_8);
  }

  public DateDistributionConfig getDistributionConfig() {
    return _distributionConfig;
  }

  public static DateVariable assertType(Variable variable) {
    if (variable instanceof DateVariable) return (DateVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a date variable.");
  }
}
