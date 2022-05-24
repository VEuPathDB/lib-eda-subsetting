package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.NumberVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.NUMBER_VALUE_COL_NAME;

public class NumberRangeFilter extends SingleValueFilter<NumberVariable> {

  private Number _min;
  private Number _max;
  
  public NumberRangeFilter(String appDbSchema, Entity entity, NumberVariable variable, Number min, Number max) {
    super(appDbSchema, entity, variable);
    _min = min;
    _max = max;
  }

  // safe from SQL injection since input classes are Number
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + NUMBER_VALUE_COL_NAME + " >= " + _min + " AND " + NUMBER_VALUE_COL_NAME + " <= " + _max + NL;
  }

}
