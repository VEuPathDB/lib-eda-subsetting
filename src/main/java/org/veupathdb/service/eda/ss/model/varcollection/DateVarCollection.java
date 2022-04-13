package org.veupathdb.service.eda.ss.model.varcollection;

import java.util.List;
import org.veupathdb.service.eda.generated.model.BinUnits;
import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public class DateVarCollection extends VarCollection {

  private final DateDistributionConfig _distributionConfig;

  public DateVarCollection(Properties collectionProperties, DateDistributionConfig distributionConfig) {
    super(collectionProperties);
    _distributionConfig = distributionConfig;
  }

  public DateDistributionConfig getDistributionConfig() {
    return _distributionConfig;
  }

  @Override
  protected void assignDistributionDefaults(List<VariableWithValues> memberVars) {
    int maxBinSize = 1; // find the biggest size
    int maxBinUnitsOrdinal = 0; // find the biggest units
    for (VariableWithValues var : memberVars) {
      // superclass promises to only pass the correct type here
      DateDistributionConfig varConfig = ((DateVariable)var).getDistributionConfig();
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
