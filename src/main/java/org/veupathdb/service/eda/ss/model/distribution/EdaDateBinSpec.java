package org.veupathdb.service.eda.ss.model.distribution;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.distribution.DateBinDistribution.DateBinSpec;
import org.veupathdb.service.eda.ss.Utils;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

import static org.gusdb.fgputil.functional.Functions.doThrow;

public class EdaDateBinSpec implements DateBinSpec {

  private final DateVariable _variable;
  private final Optional<BinSpecWithRange> _binSpec;

  public EdaDateBinSpec(DateVariable variable, Optional<BinSpecWithRange> binSpec) {
    _variable = variable;
    _binSpec = binSpec;

    // if bin spec is sent, the bin units inside must have a value; other values are required by RAML
    if (binSpec.isPresent() && binSpec.get().getBinUnits() == null) {
      throw new BadRequestException("binUnits is required for date variable distributions");
    }
  }

  public ChronoUnit getBinUnits() {
    return convertToChrono(_binSpec
        .map(spec -> spec.getBinUnits())
        .orElse(_variable.getDistributionConfig().getDefaultBinUnits()));
  }

  @Override
  public int getBinSize() {
    return _binSpec
        .map(spec -> spec.getBinWidth().intValue())
        .orElse(_variable.getDistributionConfig().binSize);
  }

  @Override
  public String getDisplayRangeMin() {
    return _binSpec
        .map(spec -> Utils.standardizeLocalDateTime(castToString(spec.get_displayRangeMin())))
        .orElse(_variable.getDistributionConfig().displayRangeMin);
  }

  @Override
  public String getDisplayRangeMax() {
    return _binSpec
        .map(spec -> Utils.standardizeLocalDateTime(castToString(spec.getDisplayRangeMax())))
        .orElse(_variable.getDistributionConfig().displayRangeMax);
  }

  private static String castToString(Object rangeBoundary) {
    return (rangeBoundary == null || rangeBoundary instanceof String)
        ? (String)rangeBoundary : doThrow(() -> new BadRequestException(
            "Date range boundary must be a date-formatted string value."));
  }

  private static ChronoUnit convertToChrono(BinUnits binUnits) {
    // convert to ChronoUnit for use in adjusting min/max and bin sizes
    return switch(binUnits) {
      case DAY -> ChronoUnit.DAYS;
      case WEEK -> ChronoUnit.WEEKS;
      case MONTH -> ChronoUnit.MONTHS;
      case YEAR -> ChronoUnit.YEARS;
    };
  }
}
