package org.veupathdb.service.eda.ss.model.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.gusdb.fgputil.FormatUtil.NL;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.BinaryValuesStreamer;
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

  public List<MultiFilterSubFilter> getSubFilters() {
    return subFilters;
  }

  public MultiFilterOperation getOperation() {
    return operation;
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

  @Override
  public CloseableIterator<Long> streamFilteredIds(BinaryValuesStreamer binaryValuesStreamer, Study study) {
    try {
      return binaryValuesStreamer.streamMultiFilteredEntityIdIndexes(this, study);
    } catch (IOException e) {
      throw new RuntimeException("IO operation failed while trying to stream filtered IDs.", e);
    }
  }

  public MultiFilter(String appDbSchema, Entity entity, List<MultiFilterSubFilter> subFilters, MultiFilterOperation operation) {
    super(appDbSchema, entity);
    this.subFilters = subFilters;
    this.operation = operation;
  }

  public StringSetFilter getFilter(MultiFilterSubFilter subFilter) {
    return new StringSetFilter(_appDbSchema, _entity, subFilter.getVariable(), subFilter.getStringSet());
  }

  private String getSingleFilterSql(MultiFilterSubFilter subFilter) {
    StringVariable stringVar = StringVariable.assertType(subFilter.getVariable());
    StringSetFilter ssf = new StringSetFilter(_appDbSchema, _entity, stringVar, subFilter.getStringSet());
    return ssf.getSql();
  }

}
