package org.veupathdb.service.eda.ss.model.filter;

import java.util.ArrayList;
import java.util.List;

import static org.gusdb.fgputil.FormatUtil.NL;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.StringVariable;

public class MultiFilter extends Filter {

  private final List<MultiFilterSubFilter> subFilters;
  private final MultiFilterOperation operation;

  public enum MultiFilterOperation {
    UNION("UNION"), INTERSECT("INTERSECT");

    private final String operation;

    MultiFilterOperation(String operation) {
      this.operation = operation;
    }

    String getOperation() {return operation;}

    public static MultiFilterOperation fromString(String operation) {
      switch (operation) {
        case "intersect": return INTERSECT;
        case "union": return UNION;
        default: throw new RuntimeException("Unrecognized multi-filter operation: " + operation);
      }
    }
  }

  @Override
  public String getSql() {
    List<String> subFiltersSqlList = new ArrayList<String>();
    for (MultiFilterSubFilter subFilter : subFilters) subFiltersSqlList.add(getSingleFilterSql(subFilter));
    String subFiltersSql = String.join("  " + operation.getOperation() + NL, subFiltersSqlList);
    return "  select * from ( -- START OF MULTIFILTER" + NL
        + subFiltersSql + NL
        + "  ) -- END OF MULTIFILTER" + NL;
  }

  public MultiFilter(String appDbSchema, Entity entity, List<MultiFilterSubFilter> subFilters, MultiFilterOperation operation) {
    super(appDbSchema, entity);
    this.subFilters = subFilters;
    this.operation = operation;
  }

  private String getSingleFilterSql(MultiFilterSubFilter subFilter) {
    StringVariable stringVar = StringVariable.assertType(subFilter.getVariable());
    StringSetFilter ssf = new StringSetFilter(_appDbSchema, _entity, stringVar, subFilter.getStringSet());
    return ssf.getSql();
  }

}
