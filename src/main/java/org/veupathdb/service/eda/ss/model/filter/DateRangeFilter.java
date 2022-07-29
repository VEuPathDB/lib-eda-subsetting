package org.veupathdb.service.eda.ss.model.filter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME;

public class DateRangeFilter extends SingleValueFilter<Long, DateVariable> {

  private final LocalDateTime _min;
  private final LocalDateTime _max;
  
  public DateRangeFilter(String appDbSchema, Entity entity, DateVariable variable, LocalDateTime min, LocalDateTime max) {
    super(appDbSchema, entity, variable);
    _min = min;
    _max = max;
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + DATE_VALUE_COL_NAME + " >= " + dbDateTimeIsoValue(_min) + " AND " + DATE_VALUE_COL_NAME + " <= " + dbDateTimeIsoValue(_max) + NL;
  }

  @Override
  public Predicate<Long> getPredicate() {
    return date -> date >= _min.toInstant(ZoneOffset.UTC).toEpochMilli()
        && date <= _max.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  static String dbDateTimeIsoValue(LocalDateTime dateTime) {
    String str = dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    return "TO_DATE('" + str + "', 'YYYY-MM-DD\"T\"HH24:MI:SS')";
  }
}
