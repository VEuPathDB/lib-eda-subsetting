package org.veupathdb.service.eda.ss.model.distribution;

import org.veupathdb.service.eda.generated.model.HistogramStats;
import org.veupathdb.service.eda.generated.model.HistogramStatsImpl;

public class StatsCollector<T> {

  protected T _subsetMin;
  protected T _subsetMax;
  private long _numValues = 0;
  private long _numDistinctValues = 0;

  public void accept(T value, Long count) {
    if (_subsetMin == null) _subsetMin = value;
    _subsetMax = value;
    _numValues += count;
    _numDistinctValues++;
  }

  public HistogramStats toHistogramStats(long subsetEntityCount, long missingCasesCount) {
    HistogramStats stats = new HistogramStatsImpl();
    stats.setSubsetMin(_subsetMin);
    stats.setSubsetMax(_subsetMax);
    stats.setSubsetSize(subsetEntityCount);
    stats.setNumVarValues(_numValues);
    stats.setNumDistinctValues(_numDistinctValues);
    stats.setNumDistinctEntityRecords(subsetEntityCount - missingCasesCount);
    stats.setNumMissingCases(missingCasesCount);
    return stats;
  }
}
