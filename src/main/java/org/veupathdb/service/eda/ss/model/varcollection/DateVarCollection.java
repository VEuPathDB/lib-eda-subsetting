package org.veupathdb.service.eda.ss.model.varcollection;

import java.time.LocalDateTime;
import java.util.List;

import org.veupathdb.service.eda.ss.model.distribution.BinUnits;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

public class DateVarCollection extends VarCollection<LocalDateTime, DateVariable> {

  private final DateDistributionConfig _distributionConfig;

  public DateVarCollection(Properties collectionProperties, DateDistributionConfig distributionConfig) {
    super(collectionProperties);
    _distributionConfig = distributionConfig;
  }

  public DateDistributionConfig getDistributionConfig() {
    return _distributionConfig;
  }

  @Override
  protected void assignDistributionDefaults(List<DateVariable> memberVars) {
    int maxBinSize = 1; // find the biggest size
    int maxBinUnitsOrdinal = 0; // find the biggest units
    for (DateVariable var : memberVars) {
      // superclass promises to only pass the correct type here
      DateDistributionConfig varConfig = var.getDistributionConfig();
      if (varConfig.binSize > maxBinSize) {
        maxBinSize = varConfig.binSize;
      }
      if (varConfig.getDefaultBinUnits().ordinal() > maxBinUnitsOrdinal) {
        maxBinUnitsOrdinal = varConfig.getDefaultBinUnits().ordinal();
      }
    }
    _distributionConfig.binSize = maxBinSize;
    _distributionConfig.binUnits = BinUnits.values()[maxBinUnitsOrdinal];
  }
}
