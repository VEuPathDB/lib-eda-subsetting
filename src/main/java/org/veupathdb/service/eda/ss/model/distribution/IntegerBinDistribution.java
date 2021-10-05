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
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.IntegerVariable;

public class IntegerBinDistribution extends NumberBinDistribution<Long> {

  public IntegerBinDistribution(DataSource ds, Study study, Entity targetEntity, IntegerVariable var,
                                List<Filter> filters, ValueSpec valueSpec, Optional<BinSpecWithRange> binSpec) {
    super(ds, study, targetEntity, var, filters, valueSpec, binSpec);
  }

  @Override
  protected Long sum(Long a, Long b) {
    return a + b;
  }

  @Override
  protected Long getTypedObject(String objectName, Object value, ValueSource source) {
    Supplier<RuntimeException> exSupplier = () -> switch(source) {
      case CONFIG -> new BadRequestException(objectName + " must be an integer value.");
      case DB -> new RuntimeException("Value in column " + objectName + " is not an integer.");
    };
    if (value instanceof Number) {
      return ((Number)value).longValue();
    }
    if (value instanceof String) {
      try {
        return Long.parseLong((String)value);
      }
      catch (NumberFormatException e) {
        throw exSupplier.get();
      }
    }
    throw exSupplier.get();
  }

  @Override
  protected StatsCollector<Long> getStatsCollector() {
    return new StatsCollector<>() {

      private Long _sumOfValues = 0L;

      @Override
      public void accept(Long value, Long count) {
        super.accept(value, count);
        _sumOfValues += (count * value);
      }

      @Override
      public HistogramStats toHistogramStats(long subsetEntityCount, long missingCasesCount) {
        HistogramStats stats = super.toHistogramStats(subsetEntityCount, missingCasesCount);
        stats.setSubsetMean(_sumOfValues.doubleValue() / stats.getNumVarValues());
        return stats;
      }
    };
  }

}
