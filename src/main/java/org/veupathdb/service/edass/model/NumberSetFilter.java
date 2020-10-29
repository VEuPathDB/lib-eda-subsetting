package org.veupathdb.service.edass.model;

import java.util.List;
import java.util.stream.Collectors;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;

public class NumberSetFilter extends Filter {

  private List<Number> numberSet;
  
  public NumberSetFilter(Entity entity, String variableId, List<Number> numberSet) {
    super(entity, variableId);
    this.numberSet = numberSet;
  }

  @Override
  public String getAndClausesSql() {
    List<String> vals = numberSet.stream().map(n -> String.valueOf(n)).collect(Collectors.toList());
    return "  AND " + NUMBER_VALUE_COL_NAME + " IN (" + String.join(", ", vals) + " )" + NL;
  }

}
