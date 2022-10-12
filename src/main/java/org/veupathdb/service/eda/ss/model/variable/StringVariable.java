package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.FormatUtil;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.ByteArrayConverter;

public class StringVariable extends VariableWithValues<byte[]> {
  private final static int LARGEST_STRING = 200;

  public StringVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties) {
    super(varProperties, valueProperties);
    validateType(VariableType.STRING);
  }
  
  // static version for use when we don't have an instance
  public static BinaryConverter<byte[]> getGenericBinaryConverter() {
    return new ByteArrayConverter(LARGEST_STRING);
  }
  
  @Override
  public BinaryConverter<byte[]> getBinaryConverter() {
    return getGenericBinaryConverter();
  }

  @Override
  public byte[] fromString(String s) {
    return FormatUtil.stringToPaddedBinary(s, LARGEST_STRING);
  }

  @Override
  public String valueToString(byte[] val, TabularReportConfig reportConfig) {
    return FormatUtil.paddedBinaryToString(val);
  }

  @Override
  public String valueToJsonText(byte[] val, TabularReportConfig config) {
    return quote(valueToString(val, config));
  }

  public static StringVariable assertType(Variable variable) {
    if (variable instanceof StringVariable) return (StringVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a string variable.");
  }
}
