package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import java.util.Comparator;
import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

public class NumberRangeFilter<T extends Number & Comparable> extends SingleValueFilter<NumberVariable, T> {

  private Number _min;
  private Number _max;

  public NumberRangeFilter(String appDbSchema, Entity entity, NumberVariable<T> variable, Number min, Number max) {
    super(appDbSchema, entity, variable);
    _min = min;
    _max = max;
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " >= " + _min + " AND " + NUMBER_VALUE_COL_NAME + " <= " + _max + NL;
  }

  @Override
  public Predicate<T> getPredicate() {
    return n -> n.compareTo(_min) >= 0 && n.compareTo(_max) <= 0;
  }
}
