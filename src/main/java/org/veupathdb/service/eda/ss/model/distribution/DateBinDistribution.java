package org.veupathdb.service.eda.ss.model.distribution;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.HistogramStats;
import org.veupathdb.service.eda.generated.model.HistogramStatsImpl;
import org.veupathdb.service.eda.generated.model.ValueSpec;
import org.veupathdb.service.eda.ss.model.DateVariable;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.distribution.BinHelpers.DateBin;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.service.RequestBundle;

public class DateBinDistribution extends AbstractBinDistribution<DateVariable, LocalDateTime, DateBin> {

  private final TemporalUnit _binUnits;
  private final int _binSize;

  public DateBinDistribution(DataSource ds, Study study, Entity targetEntity, DateVariable var,
                             List<Filter> filters, ValueSpec valueSpec, BinSpecWithRange binSpec) {
    super(ds, study, targetEntity, var, filters, valueSpec, binSpec.getDisplayRangeMin(), binSpec.getDisplayRangeMax());
    _binUnits = switch(binSpec.getBinUnits()) {
      case DAY -> ChronoUnit.DAYS;
      case WEEK -> ChronoUnit.WEEKS;
      case MONTH -> ChronoUnit.MONTHS;
      case YEAR -> ChronoUnit.YEARS;
    };
    _binSize = binSpec.getBinWidth().intValue();
  }

  @Override
  protected LocalDateTime getTypedObject(String objectName, Object value, ValueSource source) {
    if (value instanceof String) {
      return RequestBundle.parseDate((String)value);
    }
    throw switch(source) {
      case CONFIG -> new BadRequestException(objectName + " must be a date string value.");
      case DB -> new RuntimeException("Converted value in column " + objectName + " is not a valid date string.");
    };
  }

  @Override
  protected StatsCollector getStatsCollector() {
    return new StatsCollector() {

      private LocalDateTime _subsetMin;
      private LocalDateTime _subsetMax;
      private long _numValues = 0;
      private long _sumOfValues = 0;

      @Override
      void accept(LocalDateTime value, Long count) {
        if (_subsetMin == null) _subsetMin = value;
        _subsetMax = value;
        _numValues += count;
        _sumOfValues += (count * value.toEpochSecond(ZoneOffset.UTC));
      }

      @Override
      HistogramStats toHistogramStats(int uniqueEntityCount) {
        HistogramStats stats = new HistogramStatsImpl();
        stats.setSubsetMin(RequestBundle.formatDate(_subsetMin));
        stats.setSubsetMax(RequestBundle.formatDate(_subsetMax));
        stats.setSubsetMean(RequestBundle.formatDate(
            LocalDateTime.ofEpochSecond(_sumOfValues / _numValues, 0, ZoneOffset.UTC)));
        stats.setNumVarValues((int)_numValues); // FIXME: int cast ok here?
        stats.setNumDistinctEntityRecords(uniqueEntityCount);
        stats.setNumMissingCases(0); // FIXME: get from null tuple?
        return stats;
      }
    };
  }

  @Override
  protected DateBin getFirstBin() {
    // truncate display min as necessary
    LocalDateTime firstBinStart = _displayMin.truncatedTo(ChronoUnit.DAYS);
    // TODO: adjust firstBinStart to be the beginning of the range for month, year
    return getNextBin(new DateBin(null, firstBinStart)).orElseThrow();
  }

  @Override
  protected Optional<DateBin> getNextBin(DateBin currentBin) {
    return _displayMax.isBefore(currentBin._end) ? Optional.empty() :
        Optional.of(new DateBin(currentBin._end, currentBin._end.plus(_binSize, _binUnits)));
  }

}
