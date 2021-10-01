package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.RdbmsColumnNames;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import static org.gusdb.fgputil.FormatUtil.NL;

public class NumberRangeFilter extends SingleValueFilter<NumberVariable> {

  private Number _min;
  private Number _max;
  
  public NumberRangeFilter(Entity entity, NumberVariable variable, Number min, Number max) {
    super(entity, variable);
    _min = min;
    _max = max;
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + RdbmsColumnNames.NUMBER_VALUE_COL_NAME + " >= " + _min + " AND " + RdbmsColumnNames.NUMBER_VALUE_COL_NAME + " <= " + _max + NL;
  }

}
