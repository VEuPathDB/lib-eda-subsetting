package org.veupathdb.service.eda.ss.model.varcollection;

import java.util.List;
import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;

public class FloatingPointVarCollection extends VarCollection<Double, FloatingPointVariable> {

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

  @Override
  protected void assignDistributionDefaults(List<FloatingPointVariable> memberVars) {
    Double maxBinSize = (double)0; // find the biggest size
    for (FloatingPointVariable var : memberVars) {
      // superclass promises to only pass the correct type here
      NumberDistributionConfig<Double> varConfig = var.getDistributionConfig();
      if (varConfig.getDefaultBinWidth() > maxBinSize) {
        maxBinSize = varConfig.getDefaultBinWidth();
      }
    }
    _distributionConfig.setBinWidth(maxBinSize);
  }
}
