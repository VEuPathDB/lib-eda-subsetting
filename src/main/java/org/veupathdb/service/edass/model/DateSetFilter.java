package org.veupathdb.service.edass.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DateSetFilter extends Filter {

  private List<LocalDateTime> dateSet;
   
  public DateSetFilter(Entity entity, String variableId, List<LocalDateTime> dateSet) {
    super(entity, variableId);
    this.dateSet = dateSet;
  }

  @Override
  public String getAndClausesSql() {
    List<String> dateStrings = new ArrayList<String>();
    for (LocalDateTime date : dateSet) dateStrings.add(date.toString());
    return "  AND date_value IN ('" + String.join("', '", dateStrings) + "')" + nl;
  }

}
