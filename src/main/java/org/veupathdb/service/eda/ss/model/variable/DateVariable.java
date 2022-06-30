package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.DateValueConverter;

import java.time.LocalDateTime;

public class DateVariable extends VariableWithValues<LocalDateTime> {

  private final DateDistributionConfig _distributionConfig;

  public DateVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties, DateDistributionConfig distributionConfig) {
    super(varProperties, valueProperties);
    _distributionConfig = distributionConfig;
    validateType(VariableType.DATE);
  }

  public static BinaryConverter<LocalDateTime> getGenericBinaryConverter() {
    return new DateValueConverter();
  }

  @Override
  public BinaryConverter<LocalDateTime> getBinaryConverter() {
    return getGenericBinaryConverter();
  }

  @Override
  public LocalDateTime fromString(String s) {
    return FormatUtil.parseDateTime(s);
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
