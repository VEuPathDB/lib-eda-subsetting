package org.veupathdb.service.eda.ss.model.variable;

import org.veupathdb.service.eda.ss.model.distribution.NumberDistributionConfig;

/**
 * This is a superclass supporting ONLY float and integer types, which can both
 * be used in NumberRange and NumberSet filters and support numeric range distributions.
 */
public abstract class NumberVariable<T extends Number & Comparable<T>> extends VariableWithValues<T> {

  public abstract T toNumberSubtype(Number number);
  public abstract T validateBinWidth(Number binWidth);
  public abstract String getUnits();

  private final NumberDistributionConfig<T> _distributionConfig;

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
