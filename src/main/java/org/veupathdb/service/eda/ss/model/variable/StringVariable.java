package org.veupathdb.service.eda.ss.model.variable;

import jakarta.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.variable.converter.StringValueConverter;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueConverter;

public class StringVariable extends VariableWithValues<String> {

  public StringVariable(Variable.Properties varProperties, VariableWithValues.Properties valueProperties) {
    super(varProperties, valueProperties);
    validateType(VariableType.STRING);
  }

  @Override
  public ValueConverter<String> getValueConverter() {
    return new StringValueConverter(100);
  }

  public static StringVariable assertType(Variable variable) {
    if (variable instanceof StringVariable) return (StringVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a string variable.");
  }
}
