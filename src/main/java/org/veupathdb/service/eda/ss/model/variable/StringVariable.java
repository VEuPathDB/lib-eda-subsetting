package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.StringValueConverter;

public class StringVariable extends VariableWithValues<String> {
  private final static int LARGEST_STRING = 200;

  public StringVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties) {
    super(varProperties, valueProperties);
    validateType(VariableType.STRING);
  }
  
  // static version for use when we don't have an instance
  public static BinaryConverter<String> getGenericBinaryConverter() {
    return new StringValueConverter(LARGEST_STRING);
  }
  
  @Override
  public BinaryConverter<String> getBinaryConverter() {
    return getGenericBinaryConverter();
  }

  @Override
  public String fromString(String s) {
    return s;
  }

  public static StringVariable assertType(Variable variable) {
    if (variable instanceof StringVariable) return (StringVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a string variable.");
  }
}
