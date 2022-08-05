package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DateValueConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateVariable extends VariableWithValues<Long> {
  private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_DATE;
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

  private final DateDistributionConfig _distributionConfig;

  public DateVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, DateDistributionConfig distributionConfig) {
    super(varProperties, valueProperties);
    _distributionConfig = distributionConfig;
    validateType(VariableType.DATE);
  }

  public static BinaryConverter<Long> getGenericBinaryConverter() {
    return new LongValueConverter();
  }

  @Override
  public BinaryConverter<Long> getBinaryConverter() {
    return getGenericBinaryConverter();
  }

  @Override
  public Long fromString(String s) {
    return FormatUtil.parseDateTime(s).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Override
  public String valueToString(Long val, TabularReportConfig reportConfig) {
    if (reportConfig.getTrimTimeFromDateVars()) {
      return DATE_FORMATTER.format(Instant.ofEpochMilli(val));
    }
    return DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(val));
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
