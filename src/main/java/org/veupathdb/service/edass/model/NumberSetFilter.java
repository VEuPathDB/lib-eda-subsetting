package org.veupathdb.service.edass.model;

import java.util.List;
import java.util.stream.Collectors;

import org.veupathdb.service.edass.generated.model.APINumberSetFilter;

public class NumberSetFilter extends Filter {

  private APINumberSetFilter inputFilter;
  
  public NumberSetFilter(APINumberSetFilter inputFilter, String entityId, String entityPrimaryKeyColumunName, String entityTableName) {
    super(entityId, entityPrimaryKeyColumunName, entityTableName, inputFilter.getVariableId());
    this.inputFilter = inputFilter;
  }

  @Override
  public String getAndClausesSql() {
    List<String> vals = inputFilter.getNumberSet().stream().map(n -> String.valueOf(n)).collect(Collectors.toList());
    return "AND number_value IN (" + String.join(", ", vals) + " )" + nl;
  }

}
