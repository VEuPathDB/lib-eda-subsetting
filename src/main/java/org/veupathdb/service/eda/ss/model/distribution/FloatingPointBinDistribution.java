package org.veupathdb.service.eda.ss.model.distribution;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.HistogramStats;
import org.veupathdb.service.eda.generated.model.HistogramStatsImpl;
import org.veupathdb.service.eda.generated.model.ValueSpec;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.FloatingPointVariable;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;

public class FloatingPointBinDistribution extends NumberBinDistribution<Double> {

  public FloatingPointBinDistribution(DataSource ds, Study study, Entity targetEntity, FloatingPointVariable var,
                                      List<Filter> filters, ValueSpec valueSpec, Optional<BinSpecWithRange> binSpec) {
    super(ds, study, targetEntity, var, filters, valueSpec, binSpec);
  }

  @Override
  protected Double sum(Double a, Double b) {
    return a + b;
  }

  @Override
  protected Double getTypedObject(String objectName, Object value, ValueSource source) {
    Supplier<RuntimeException> exSupplier = () -> switch(source) {
      case CONFIG -> new BadRequestException(objectName + " must be a number value.");
      case DB -> new RuntimeException("Value in column " + objectName + " is not a number.");
    };
    if (value instanceof Number) {
      return ((Number)value).doubleValue();
    }
    if (value instanceof String) {
      try {
        return Double.parseDouble((String)value);
      }
      catch (NumberFormatException e) {
        throw exSupplier.get();
      }
    }
    throw exSupplier.get();
  }

  @Override
  protected StatsCollector<Double> getStatsCollector() {
    return new StatsCollector<>() {

      private double _sumOfValues = 0;

      @Override
      public void accept(Double value, Long count) {
        super.accept(value, count);
        _sumOfValues += (count * value);
      }

      @Override
      public HistogramStats toHistogramStats(long subsetEntityCount, long missingCasesCount) {
        HistogramStats stats = super.toHistogramStats(subsetEntityCount, missingCasesCount);
        stats.setSubsetMean(_sumOfValues / stats.getNumVarValues());
        return stats;
      }
    };
  }

}
