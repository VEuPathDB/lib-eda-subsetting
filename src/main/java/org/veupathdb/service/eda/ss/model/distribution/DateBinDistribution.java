package org.veupathdb.service.eda.ss.model.distribution;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.BinUnits;
import org.veupathdb.service.eda.generated.model.HistogramStats;
import org.veupathdb.service.eda.generated.model.ValueSpec;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;
import org.veupathdb.service.eda.ss.service.RequestBundle;

public class DateBinDistribution extends AbstractBinDistribution<DateVariable, LocalDateTime, DateBin> {

  private static final Logger LOG = LogManager.getLogger(DateBinDistribution.class);

  private final LocalDateTime _displayMin;
  private final LocalDateTime _displayMax;
  private final int _binSize;
  private final ChronoUnit _binUnits;

  public DateBinDistribution(DataSource ds, Study study, Entity targetEntity, DateVariable var,
                             List<Filter> filters, ValueSpec valueSpec, Optional<BinSpecWithRange> binSpec) {
    super(ds, study, targetEntity, var, filters, valueSpec);

    // process the bin spec to configure this distribution
    _binUnits = getBinUnits(binSpec);
    _binSize = getBinSize(binSpec);
    _displayMin = getDisplayMin(binSpec);
    _displayMax = getDisplayMax(binSpec);
  }

  private ChronoUnit getBinUnits(Optional<BinSpecWithRange> binSpec) {
    // if bin spec is sent, the bin units inside must have a value
    if (binSpec.isPresent() && binSpec.get().getBinUnits() == null) {
      throw new BadRequestException("binUnits is required for date variable distributions");
    }
    // use submitted value or value from var
    BinUnits binUnits = binSpec.map(spec -> spec.getBinUnits()).orElse(_variable.getBinUnits());
    // convert to ChronoUnit for use in adjusting min/max and bin sizes
    return switch(binUnits) {
      case DAY -> ChronoUnit.DAYS;
      case WEEK -> ChronoUnit.WEEKS;
      case MONTH -> ChronoUnit.MONTHS;
      case YEAR -> ChronoUnit.YEARS;
    };
  }

  private int getBinSize(Optional<BinSpecWithRange> binSpec) {
    int binSize = binSpec.map(spec -> spec.getBinWidth().intValue()).orElse(_variable.getBinSize());
    if (binSize <= 0) {
      throw new BadRequestException("binWidth must be a positive integer for date variable distributions");
    }
    return binSize;
  }

  private LocalDateTime getDisplayMin(Optional<BinSpecWithRange> binSpec) {
    Object rawMinValue = binSpec.map(spec -> spec.getDisplayRangeMin()).orElse(_variable.getDisplayRangeMin());
    LocalDateTime rawDateTime = getTypedObject("displayRangeMin", rawMinValue, ValueSource.CONFIG);
    switch(_binUnits) {
      case MONTHS:
        // truncate to the beginning of the month
        return LocalDateTime.of(rawDateTime.getYear(), rawDateTime.getMonth(), 1, 0, 0);
      case YEARS:
        // truncate to the beginning of the year
        return LocalDateTime.of(rawDateTime.getYear(), Month.JANUARY, 1, 0, 0);
      default:
        // for days and weeks, truncate to the beginning of the day
        return rawDateTime.truncatedTo(ChronoUnit.DAYS);
    }
  }

  private LocalDateTime getDisplayMax(Optional<BinSpecWithRange> binSpec) {
    Object rawMaxValue = binSpec.map(spec -> spec.getDisplayRangeMax()).orElse(_variable.getDisplayRangeMax());
    LocalDateTime rawDateTime = getTypedObject("displayMax", rawMaxValue, ValueSource.CONFIG);
    // truncate to the day (floor), then add 1 day and subtract 1 second
    //    this way all values on that day fall before the max, but none after
    return rawDateTime
        .truncatedTo(ChronoUnit.DAYS)
        .plus(1, ChronoUnit.DAYS)
        .minus(1, ChronoUnit.SECONDS);
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
  protected StatsCollector<LocalDateTime> getStatsCollector() {
    return new StatsCollector<>() {

      private long _sumOfValues = 0;

      @Override
      public void accept(LocalDateTime value, Long count) {
        super.accept(value, count);
        _sumOfValues += (count * value.toEpochSecond(ZoneOffset.UTC));
      }

      @Override
      public HistogramStats toHistogramStats(long subsetEntityCount, long missingCasesCount) {
        HistogramStats stats = super.toHistogramStats(subsetEntityCount, missingCasesCount);
        // override the LocalDateTime objects set by the parent class and assign strings
        // check for null to handle the case where prev filters leave no data for the current variable
        if (_subsetMin != null) {
          stats.setSubsetMin(RequestBundle.formatDate(_subsetMin));
          stats.setSubsetMax(RequestBundle.formatDate(_subsetMax));
          stats.setSubsetMean(RequestBundle.formatDate(
              LocalDateTime.ofEpochSecond(_sumOfValues / stats.getNumVarValues(), 0, ZoneOffset.UTC)));
        }
        return stats;
      }
    };
  }

  @Override
  protected DateBin getFirstBin() {
    return getNextBin(new DateBin(null, _displayMin, _binUnits, _binSize)).orElseThrow();
  }

  @Override
  protected Optional<DateBin> getNextBin(DateBin currentBin) {
    return _displayMax.isBefore(currentBin._end) ? Optional.empty() :
        Optional.of(new DateBin(currentBin._end, currentBin._end.plus(_binSize, _binUnits), _binUnits, _binSize));
  }

}
