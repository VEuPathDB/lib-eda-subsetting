package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.edass.model.RdbmsColumnNames.DATE_VALUE_COL_NAME;

public class DateRangeFilter extends Filter {

  private LocalDateTime min;
  private LocalDateTime max;
  
  public DateRangeFilter(Entity entity, String variableId, LocalDateTime min, LocalDateTime max) {
    super(entity, variableId);
    this.min = min;
    this.max = max;
  }

  @Override
  public String getAndClausesSql() {
    return "  AND " + DATE_VALUE_COL_NAME + " >= " + dbDateTimeIsoValue(min) + " AND " + DATE_VALUE_COL_NAME + " <= " + dbDateTimeIsoValue(max) + NL;
  }

  static String dbDateTimeIsoValue(LocalDateTime dateTime) {
    String str = dateTime.format(DateTimeFormatter.ISO_DATE_TIME);
    return "TO_DATE('" + str + "', 'YYYY-MM-DD\"T\"HH24:MI:SS')";
  }
}
