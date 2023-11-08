package org.veupathdb.service.eda.subset.model.varcollection;

import java.util.List;
import org.veupathdb.service.eda.subset.model.distribution.NumberDistributionConfig;
import org.veupathdb.service.eda.subset.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.subset.model.variable.NumberVariable;

/**
 *
 * @param <T> T is the type of the variables contained in this collection. Note that floating point collections can
 *            contain FloatingPoint or Integer variables, hence T being compatible with both of these types instead of just floats.
 */
public class FloatingPointVarCollection<T extends Number & Comparable<T>> extends VarCollection<T, NumberVariable<T>> {

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
  protected void assignDistributionDefaults(List<NumberVariable<T>> memberVars) {
    double maxBinSize = 0.0; // find the biggest size
    for (NumberVariable<T> var : memberVars) {
      // superclass promises to only pass the correct type here
      NumberDistributionConfig<T> varConfig = var.getDistributionConfig();
      if (varConfig.getDefaultBinWidth().doubleValue() > maxBinSize) {
        maxBinSize = varConfig.getDefaultBinWidth().doubleValue();
      }
    }
    _distributionConfig.setBinWidth(maxBinSize);
  }
}
