package org.veupathdb.service.eda.ss.model.variable;

import javax.ws.rs.BadRequestException;
import org.veupathdb.service.eda.ss.model.distribution.DistributionConfig;

public class IntegerVariable extends NumberVariable<Integer> {

  public static class Properties {

    public final String units;

    public Properties(String units) {
      this.units = units;
    }
  }

  private final Properties _properties;

  public IntegerVariable(
      Variable.Properties varProperties,
      VariableWithValues.Properties valueProperties,
      DistributionConfig<Integer> distributionConfig,
      Properties properties) {

    super(varProperties, valueProperties, distributionConfig);
    _properties = properties;
    validateType(VariableType.INTEGER);

    String errPrefix = "In entity " + varProperties.entity.getId() + " variable " + varProperties.id + " has a null ";
    if (_properties.units == null)
      throw new RuntimeException(errPrefix + "units");

  }

  public String getUnits() {
    return _properties.units;
  }

  @Override
  public Integer validateBinWidth(Number binWidth) {
    Integer intValue = binWidth.intValue();
    if (intValue <= 0) {
      throw new BadRequestException("binWidth must be a positive integer for integer variable distributions");
    }
    return intValue;
  }

}
