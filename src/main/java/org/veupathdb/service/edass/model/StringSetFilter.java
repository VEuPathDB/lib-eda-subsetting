package org.veupathdb.service.edass.model;

import java.util.List;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.edass.model.RdbmsColumnNames.*;


public class StringSetFilter extends Filter {

  private List<String> stringSet;
  
  public StringSetFilter(Entity entity, String variableId, List<String> stringSet) {
    super(entity, variableId);
    this.stringSet = stringSet;
  }

  @Override
  public String getAndClausesSql() {
    return "  AND " + STRING_VALUE_COL_NAME + " IN ('" + String.join("', '", stringSet) + "')" + NL;
  }

}
