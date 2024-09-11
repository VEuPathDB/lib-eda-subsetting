package org.veupathdb.service.eda.subset.model.variable;

import org.veupathdb.service.eda.subset.model.distribution.NumberDistributionConfig;

/**
 * This is a superclass supporting ONLY float and integer types, which can both
 * be used in NumberRange and NumberSet filters and support numeric range distributions.
 */
public abstract class NumberVariable<T extends Number & Comparable<T>> extends VariableWithValues<T> {

  public enum InclusiveRangeBoundary {
    MIN, MAX
  }

  public abstract T getValidatedSubtype(Number value);
  public abstract T getValidatedSubtypeForInclusiveRangeBoundary(Number number, InclusiveRangeBoundary boundary);
  public abstract T getValidatedSubtypeForBinWidth(Number binWidth);
  public abstract String getUnits();

  protected final NumberDistributionConfig<T> _distributionConfig;

  public NumberVariable(Variable.Properties varProperties, Properties properties, NumberDistributionConfig<T> distributionConfig) {
    super(varProperties, properties);
    _distributionConfig = distributionConfig;
  }

  public NumberDistributionConfig<T> getDistributionConfig() {
    return _distributionConfig;
  }

  @Override
  public String getDownloadColHeader() {
    String units = getUnits();
    String unitsStr = units == null || units.isBlank() ? "" : " (" + units.trim() + ")";
    return getDisplayName() + unitsStr + " [" + getId() + "]";
  }
}
