package org.veupathdb.service.eda.subset.model.filter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Predicate;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.db.DB;

import static org.gusdb.fgputil.FormatUtil.NL;

public class DateRangeFilter extends SingleValueFilter<Long, DateVariable> {

  private final LocalDateTime _min;
  private final LocalDateTime _max;
  private final Long _minEpochMillis;
  private final Long _maxEpochMillis;

  public DateRangeFilter(String appDbSchema, Entity entity, DateVariable variable, LocalDateTime min, LocalDateTime max) {
    super(appDbSchema, entity, variable);
    _min = min;
    _max = max;
    // Store off as millis since epoch for filtering with respect to binary-encoded values.
    _minEpochMillis = _min.toInstant(ZoneOffset.UTC).toEpochMilli();
    _maxEpochMillis = _max.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    return "  AND " + DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME + " >= " + dbDateTimeIsoValue(_min) + " AND " + DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME + " <= " + dbDateTimeIsoValue(_max) + NL;
  }

  @Override
  public Predicate<Long> getPredicate() {
    return date -> date >= _minEpochMillis && date <= _maxEpochMillis;
  }

  static String dbDateTimeIsoValue(LocalDateTime dateTime) {
    String str = dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    return "TO_DATE('" + str + "', 'YYYY-MM-DD\"T\"HH24:MI:SS')";
  }
}
