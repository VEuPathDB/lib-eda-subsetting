package org.veupathdb.service.eda.ss.model.variable;

import javax.ws.rs.BadRequestException;

/**
 * This is a superclass supporting ONLY float and integer types, which can both
 * be used in NumberRange and NumberSet filters.
 */
public class NumberVariable extends VariableWithValues {
  public NumberVariable(Variable.Properties varProperties, Properties properties) {
    super(varProperties, properties);
  }

  public static NumberVariable assertType(Variable variable) {
    if (variable instanceof NumberVariable) return (NumberVariable)variable;
    throw new BadRequestException("Variable " + variable.getId() +
        " of entity " + variable.getEntityId() + " is not a number or integer variable.");
  }
}
