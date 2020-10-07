package org.veupathdb.service.edass.model;

import java.util.List;
import java.util.stream.Collectors;

public class NumberSetFilter extends Filter {

  private List<Number> numberSet;
  
  public NumberSetFilter(Entity entity, String variableId, List<Number> numberSet) {
    super(entity, variableId);
    this.numberSet = numberSet;
  }

  @Override
  public String getAndClausesSql() {
    List<String> vals = numberSet.stream().map(n -> String.valueOf(n)).collect(Collectors.toList());
    return "  AND number_value IN (" + String.join(", ", vals) + " )" + nl;
  }

}
