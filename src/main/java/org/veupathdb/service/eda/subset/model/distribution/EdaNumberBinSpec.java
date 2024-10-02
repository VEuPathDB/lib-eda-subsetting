package org.veupathdb.service.eda.subset.model.distribution;

import java.util.Optional;
import jakarta.ws.rs.BadRequestException;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.distribution.NumberBinDistribution.NumberBinSpec;
import org.veupathdb.service.eda.subset.model.variable.NumberVariable;

public class EdaNumberBinSpec implements NumberBinSpec {

  private final NumberVariable<?> _variable;
  private final Optional<BinSpecWithRange> _binSpec;

  public EdaNumberBinSpec(NumberVariable<?> variable, Optional<BinSpecWithRange> binSpec) {
    _variable = variable;
    _binSpec = binSpec;
    checkBinUnits(binSpec);
  }

  @Override
  public Object getDisplayRangeMin() {
    return _binSpec.map(BinSpecWithRange::getDisplayRangeMin)
      .orElse(_variable.getDistributionConfig().getDisplayRangeMin());
  }

  @Override
  public Object getDisplayRangeMax() {
    return _binSpec
      .map(BinSpecWithRange::getDisplayRangeMax)
      .orElse(_variable.getDistributionConfig().getDisplayRangeMax());
  }

  @Override
  public Object getBinSize() {
    return _binSpec
      .map(BinSpecWithRange::getBinWidth)
      .orElse(_variable.getDistributionConfig().getDefaultBinWidth());
  }

  // don't allow binUnits to be submitted to a number distribution even though schema allows it
  private void checkBinUnits(Optional<BinSpecWithRange> binSpec) {
    Optional<BinUnits> units = binSpec.map(BinSpecWithRange::getBinUnits);
    units.ifPresent(u -> Functions.doThrow(() -> new BadRequestException(
      "For number and integer variables, only binWidth should be submitted (not binUnits).")));
  }

}
