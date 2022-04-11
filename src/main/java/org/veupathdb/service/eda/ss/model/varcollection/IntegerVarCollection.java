package org.veupathdb.service.eda.ss.model.varcollection;

import org.veupathdb.service.eda.generated.model.BinUnits;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;

public class IntegerVarCollection extends VarCollection {

  private final IntegerVariable.Properties _integerProps;
  private final NumberDistributionConfig<Long> _distributionConfig;

  public IntegerVarCollection(
      Properties collectionProperties,
      IntegerVariable.Properties integerProps,
      NumberDistributionConfig<Long> distributionConfig) {
    super(collectionProperties);
    _integerProps = integerProps;
    _distributionConfig = distributionConfig;
  }

  public String getUnits() {
    return _integerProps.units;
  }

  public NumberDistributionConfig<Long> getDistributionConfig() {
    return _distributionConfig;
  }
}
