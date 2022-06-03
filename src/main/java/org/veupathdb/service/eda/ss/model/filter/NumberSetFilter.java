package org.veupathdb.service.eda.ss.model.filter;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.db.DB;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

import static org.gusdb.fgputil.FormatUtil.NL;

public class NumberSetFilter extends SingleValueFilter<NumberVariable, Number> {

  private List<Number> _numberSet;
  
  public NumberSetFilter(String appDbSchema, Entity entity, NumberVariable variable, List<Number> numberSet) {
    super(appDbSchema, entity, variable);
    _numberSet = numberSet;
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " IN (" + createSqlInExpression() + " )" + NL;
  }

  @Override
  public Predicate<Number> getPredicate() {
    return num -> _numberSet.contains(num);
  }

  private String createSqlInExpression() {
    return _numberSet.stream()
        .map(String::valueOf)
        .collect(Collectors.joining(", "));
  }

}
