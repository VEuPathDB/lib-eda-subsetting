package org.veupathdb.service.eda.ss.model.distribution;

import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import javax.ws.rs.BadRequestException;
import org.gusdb.fgputil.functional.Functions;
import org.veupathdb.service.eda.generated.model.BinSpecWithRange;
import org.veupathdb.service.eda.generated.model.BinUnits;
import org.veupathdb.service.eda.generated.model.ValueSpec;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

public abstract class NumberBinDistribution<T extends Number & Comparable<T>> extends AbstractBinDistribution<NumberVariable<T>, T, NumberBin<T>>{

  protected final T _displayRangeMin;
  protected final T _displayRangeMax;
  protected final T _binWidth;

  // utility for less than; I always have to check what compareTo() result means
  public static <S extends Comparable<S>> boolean isLessThan(S leftOperand, S rightOperand) {
    return leftOperand.compareTo(rightOperand) < 0;
  }

  // return the sum of the two numbers
  protected abstract T sum(T a, T b);

  public NumberBinDistribution(DataSource ds, Study study, Entity targetEntity, NumberVariable<T> variable,
                               List<Filter> filters, ValueSpec valueSpec, Optional<BinSpecWithRange> binSpec) {
    super(ds, study, targetEntity, variable, filters, valueSpec);
    _displayRangeMin = getRangeMin(binSpec);
    _displayRangeMax = getRangeMax(binSpec);
    _binWidth = getBinWidth(binSpec);
    checkBinUnits(binSpec);
  }

  private T getRangeMin(Optional<BinSpecWithRange> binSpec) {
    return getTypedObject("displayRangeMin", binSpec.map(spec -> spec.getDisplayRangeMin())
        .orElse(_variable.getDistributionConfig().getDisplayRangeMin()), ValueSource.CONFIG);
  }

  private T getRangeMax(Optional<BinSpecWithRange> binSpec) {
    return getTypedObject("displayRangeMax", binSpec.map(spec -> spec.getDisplayRangeMax())
        .orElse(_variable.getDistributionConfig().getDisplayRangeMax()), ValueSource.CONFIG);
  }

  private T getBinWidth(Optional<BinSpecWithRange> binSpec) {
    return getTypedObject("binWidth", binSpec.map(spec -> spec.getBinWidth())
        .orElse(_variable.getDistributionConfig().getDefaultBinWidth()), ValueSource.CONFIG);
  }

  // don't allow binUnits to be submitted to a number distribution even though schema allows it
  private void checkBinUnits(Optional<BinSpecWithRange> binSpec) {
    Optional<BinUnits> units = binSpec.map(spec -> spec.getBinUnits());
    units.ifPresent(u -> Functions.doThrow(() -> new BadRequestException(
        "For number and integer variables, only binWidth should be submitted (not binUnits).")));
  }

  @Override
  protected NumberBin<T> getFirstBin() {
    return getNextBin(new NumberBin<>(null, _displayRangeMin)).orElseThrow();
  }

  @Override
  protected Optional<NumberBin<T>> getNextBin(NumberBin<T> currentBin) {
    return isLessThan(_displayRangeMax, currentBin._end) ? Optional.empty() :
        Optional.of(new NumberBin<T>(currentBin._end, sum(currentBin._end, _binWidth)));
  }

}
