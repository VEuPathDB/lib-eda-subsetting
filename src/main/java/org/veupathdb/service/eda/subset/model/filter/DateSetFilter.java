package org.veupathdb.service.eda.subset.model.filter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.DateVariable;
import org.veupathdb.service.eda.subset.model.db.DB;

import static org.gusdb.fgputil.FormatUtil.NL;

public class DateSetFilter extends SingleValueFilter<Long, DateVariable> {

  private final List<LocalDateTime> _dateSet;
  private final List<Long> _dateSetMillisSinceEpoch;

  public DateSetFilter(String appDbSchema, Entity entity, DateVariable variable, List<LocalDateTime> dateSet) {
    super(appDbSchema, entity, variable);
    _dateSet = dateSet;
    _dateSetMillisSinceEpoch = dateSet.stream()
        .map(date -> date.toInstant(ZoneOffset.UTC).toEpochMilli())
        .collect(Collectors.toList());
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    List<String> dateStrings = new ArrayList<>();
    for (LocalDateTime date : _dateSet) dateStrings.add(DateRangeFilter.dbDateTimeIsoValue(date));
    return "  AND " + DB.Tables.AttributeValue.Columns.DATE_VALUE_COL_NAME + " IN (" + String.join(", ", dateStrings) + ")" + NL;
  }

  @Override
  public Predicate<Long> getPredicate() {
    return dateEpochMillis -> _dateSetMillisSinceEpoch.contains(dateEpochMillis);
  }

}
