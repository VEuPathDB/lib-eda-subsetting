package org.veupathdb.service.eda.ss.model.filter;

import java.util.List;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.RdbmsColumnNames;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import static org.gusdb.fgputil.FormatUtil.NL;

public class NumberSetFilter extends SingleValueFilter<NumberVariable> {

  private List<Number> _numberSet;
  
  public NumberSetFilter(Entity entity, NumberVariable variable, List<Number> numberSet) {
    super(entity, variable);
    _numberSet = numberSet;
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + RdbmsColumnNames.NUMBER_VALUE_COL_NAME + " IN (" + createSqlInExpression() + " )" + NL;
  }

  private String createSqlInExpression() {
    return _numberSet.stream()
        .map(String::valueOf)
        .collect(Collectors.joining(", "));
  }

}
