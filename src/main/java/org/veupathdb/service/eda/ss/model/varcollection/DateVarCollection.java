package org.veupathdb.service.eda.ss.model.varcollection;

import org.veupathdb.service.eda.ss.model.distribution.DateDistributionConfig;

public class DateVarCollection extends VarCollection {

  private final DateDistributionConfig _distributionConfig;

  public DateVarCollection(Properties collectionProperties, DateDistributionConfig distributionConfig) {
    super(collectionProperties);
    _distributionConfig = distributionConfig;
  }

  public DateDistributionConfig getDistributionConfig() {
    return _distributionConfig;
  }
}
