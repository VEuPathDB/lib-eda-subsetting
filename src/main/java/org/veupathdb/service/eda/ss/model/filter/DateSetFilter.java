package org.veupathdb.service.eda.ss.model.filter;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.DateVariable;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.RdbmsColumnNames.DATE_VALUE_COL_NAME;

public class DateSetFilter extends SingleValueFilter<DateVariable> {

  private List<LocalDateTime> _dateSet;

   
  public DateSetFilter(Entity entity, DateVariable variable, List<LocalDateTime> dateSet) {
    super(entity, variable);
    _dateSet = dateSet;
  }

  // safe from SQL injection since input classes are LocalDateTime
  @Override
  public String getFilteringAndClausesSql() {
    List<String> dateStrings = new ArrayList<>();
    for (LocalDateTime date : _dateSet) dateStrings.add(DateRangeFilter.dbDateTimeIsoValue(date));
    return "  AND " + DATE_VALUE_COL_NAME + " IN (" + String.join(", ", dateStrings) + ")" + NL;
  }

}
