package org.veupathdb.service.eda.ss.model.filter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME;

public class DateSetFilter extends SingleValueFilter<LocalDateTime, DateVariable> {

  private final List<LocalDateTime> _dateSet;

  public DateSetFilter(String appDbSchema, Entity entity, DateVariable variable, List<LocalDateTime> dateSet) {
    super(appDbSchema, entity, variable);
    _dateSet = dateSet;
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    List<String> dateStrings = new ArrayList<>();
    for (LocalDateTime date : _dateSet) dateStrings.add(DateRangeFilter.dbDateTimeIsoValue(date));
    return "  AND " + DATE_VALUE_COL_NAME + " IN (" + String.join(", ", dateStrings) + ")" + NL;
  }

  @Override
  public Predicate<LocalDateTime> getPredicate() {
    return date -> _dateSet.contains(date);
  }

}
