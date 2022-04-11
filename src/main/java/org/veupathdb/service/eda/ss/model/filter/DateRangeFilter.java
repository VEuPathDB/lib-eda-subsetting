package org.veupathdb.service.eda.ss.model.filter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.db.DB;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME;

public class DateRangeFilter extends SingleValueFilter<DateVariable> {

  private LocalDateTime _min;
  private LocalDateTime _max;
  
  public DateRangeFilter(Entity entity, DateVariable variable, LocalDateTime min, LocalDateTime max) {
    super(entity, variable);
    _min = min;
    _max = max;
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + DATE_VALUE_COL_NAME + " >= " + dbDateTimeIsoValue(_min) + " AND " + DATE_VALUE_COL_NAME + " <= " + dbDateTimeIsoValue(_max) + NL;
  }

  static String dbDateTimeIsoValue(LocalDateTime dateTime) {
    String str = dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    return "TO_DATE('" + str + "', 'YYYY-MM-DD\"T\"HH24:MI:SS')";
  }
}
