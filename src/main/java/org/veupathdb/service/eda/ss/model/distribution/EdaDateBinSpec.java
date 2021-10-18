package org.veupathdb.service.eda.ss.model.distribution;

import java.time.temporal.ChronoUnit;
import java.util.Optional;
import javax.ws.rs.BadRequestException;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.BinUnits;
import org.gusdb.fgputil.distribution.DateBinDistribution.DateBinSpec;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

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
        .orElse(_variable.getBinUnits()));
  }

  @Override
  public int getBinSize() {
    return _binSpec
        .map(spec -> spec.getBinWidth().intValue())
        .orElse(_variable.getBinSize());
  }

  @Override
  public Object getDisplayRangeMin() {
    return _binSpec
        .map(spec -> spec.getDisplayRangeMin())
        .orElse(_variable.getDisplayRangeMin());
  }

  @Override
  public Object getDisplayRangeMax() {
    return _binSpec
        .map(spec -> spec.getDisplayRangeMax())
        .orElse(_variable.getDisplayRangeMax());
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
