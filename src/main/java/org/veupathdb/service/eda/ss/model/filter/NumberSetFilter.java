package org.veupathdb.service.eda.ss.model.filter;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

import static org.gusdb.fgputil.FormatUtil.NL;

public class NumberSetFilter<T extends Number & Comparable<T>> extends SingleValueFilter<T, NumberVariable<T>> {

  // keep originally submitted numbers so SQL has original values
  private final List<Number> _numberSet;

  // also convert to more specific Number types up front for performance in file filtering
  private final List<T> _typedNumberSet;
  
  public NumberSetFilter(String appDbSchema, Entity entity, NumberVariable<T> variable, List<Number> numberSet) {
    super(appDbSchema, entity, variable);
    _numberSet = numberSet;
    _typedNumberSet = _numberSet.stream()
        .map(_variable::toNumberSubtype)
        .collect(Collectors.toList());
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " IN (" + createSqlInExpression() + " )" + NL;
  }

  @Override
  public Predicate<T> getPredicate() {
    return _typedNumberSet::contains;
  }

  private String createSqlInExpression() {
    return _numberSet.stream()
        .map(String::valueOf)
        .collect(Collectors.joining(", "));
  }

}
