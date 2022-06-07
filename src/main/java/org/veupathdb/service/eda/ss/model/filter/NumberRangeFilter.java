package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

public class NumberRangeFilter<T extends Number & Comparable<T>> extends SingleValueFilter<T, NumberVariable<T>> {

  // keep originally submitted numbers so SQL has original values
  private final Number _min;
  private final Number _max;

  // also convert to more specific Number types up front for performance in file filtering
  private final T _typedMin;
  private final T _typedMax;

  public NumberRangeFilter(String appDbSchema, Entity entity, NumberVariable<T> variable, Number min, Number max) {
    super(appDbSchema, entity, variable);
    _min = min;
    _max = max;
    _typedMin = variable.toNumberSubtype(min);
    _typedMax = variable.toNumberSubtype(max);
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " >= " + _min + " AND " + NUMBER_VALUE_COL_NAME + " <= " + _max + NL;
  }

  @Override
  public Predicate<T> getPredicate() {
    return n -> n.compareTo(_typedMin) >= 0 && n.compareTo(_typedMax) <= 0;
  }
}
