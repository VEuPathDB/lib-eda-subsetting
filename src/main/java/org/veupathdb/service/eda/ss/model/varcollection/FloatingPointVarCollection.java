package org.veupathdb.service.eda.ss.model.varcollection;

import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;

public class FloatingPointVarCollection extends VarCollection {

  private final FloatingPointVariable.Properties _floatProps;
  private final NumberDistributionConfig<Double> _distributionConfig;

  public FloatingPointVarCollection(
      Properties collectionProps,
      FloatingPointVariable.Properties floatProps,
      NumberDistributionConfig<Double> distributionConfig) {
    super(collectionProps);
    _floatProps = floatProps;
    _distributionConfig = distributionConfig;
  }

  public String getUnits() {
    return _floatProps.units;
  }

  public Long getPrecision() {
    return _floatProps.precision;
  }

  public NumberDistributionConfig<Double> getDistributionConfig() {
    return _distributionConfig;
  }

}
