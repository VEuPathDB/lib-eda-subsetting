package org.veupathdb.service.eda.subset.model.distribution;

import org.veupathdb.service.eda.subset.model.variable.VariableDataShape;

public class DateDistributionConfig {

  public final String displayRangeMin;
  public final String displayRangeMax;
  public final String rangeMin;
  public final String rangeMax;
  public Integer binSize;
  public BinUnits binUnits;
  public final BinUnits binUnitsOverride;

  public DateDistributionConfig(boolean assignBinInfo, VariableDataShape dataShape, // needed for bin units calculations
                                String displayRangeMin, String displayRangeMax,
                                String rangeMin, String rangeMax, Integer binSize,
                                String binUnits, String binUnitsOverride) {
    this.displayRangeMin = displayRangeMin;
    this.displayRangeMax = displayRangeMax;
    this.rangeMin = rangeMin;
    this.rangeMax = rangeMax;

    // massage bin values based on data shape
    if (dataShape == VariableDataShape.CONTINUOUS) {
      if (assignBinInfo) {
        this.binUnits = BinUnits.valueOf(binUnits.toUpperCase());
      }
      this.binUnitsOverride = binUnitsOverride == null ? null :
          BinUnits.valueOf(binUnitsOverride.toUpperCase());
      this.binSize = binSize;
    }
    else {
      this.binUnits = null;
      this.binUnitsOverride = null;
      this.binSize = null;
    }
  }

  public void setBinUnits(BinUnits binUnits) {
    this.binUnits = binUnits;
  }

  public BinUnits getDefaultBinUnits() {
    return binUnitsOverride != null ? binUnitsOverride : binUnits;
  }
}
