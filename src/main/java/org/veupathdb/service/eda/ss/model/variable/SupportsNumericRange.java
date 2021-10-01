package org.veupathdb.service.eda.ss.model.variable;

public interface SupportsNumericRange<T extends Number> {

  T getDisplayRangeMin();

  T getDisplayRangeMax();

  T getRangeMin();

  T getRangeMax();

  T getBinWidthOverride();

  T getBinWidth();

  T getDefaultBinWidth();

  Number validateBinWidth(Number binWidth);
}
