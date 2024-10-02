package org.veupathdb.service.eda.subset.model.filter;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.reducer.BinaryValuesStreamer;
import org.veupathdb.service.eda.subset.model.variable.VariableWithValues;
import org.veupathdb.service.eda.subset.model.db.DB;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;

/**
 *
 * @param <T> VariableType that this filter is applied to.
 * @param <U> Encoded representation of variable values.
 */
public abstract class SingleValueFilter<U, T extends VariableWithValues<U>> extends Filter {

  protected final T _variable;

  public SingleValueFilter(String appDbSchema, Entity entity, T variable) {
    super(appDbSchema, entity);
    entity.getVariable(variable.getId()).orElseThrow(
        () -> new RuntimeException("Entity " + entity.getId() + " does not contain variable " + variable.getId()));
    _variable = variable;
  }

  public T getVariable() {
    return _variable;
  }

  public List<VariableWithValues<?>> getAllVariables() {
    return List.of(_variable);
  }

  @Override
  public String getSql() {
    return _entity.getAncestorPkColNames().isEmpty() ? getSqlNoAncestors() : getSqlWithAncestors();
  }

  @Override
  public CloseableIterator<Long> streamFilteredIds(BinaryValuesStreamer binaryValuesStreamer, Study study) {
    try {
      return binaryValuesStreamer.streamFilteredEntityIdIndexes(this, study);
    } catch (IOException e) {
      throw new RuntimeException("IO operation failed while trying to stream filtered IDs.", e);
    }
  }

  /**
   * subclasses provide an AND clause to find rows that match their filter
   */
  public abstract String getFilteringAndClausesSql();

  public abstract Predicate<U> getPredicate();

  // join to ancestors table to get ancestor IDs
  String getSqlWithAncestors() {

    return getSingleFilterCommonSqlWithAncestors() + NL
        + "  AND " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + " = '" + _variable.getId() + "'" + NL
        + getFilteringAndClausesSql();
  }

  String getSqlNoAncestors() {

    return getSingleFilterCommonSqlWithAncestors() + NL
        + "  AND " + DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME + " = '" + _variable.getId() + "'" + NL
        + getFilteringAndClausesSql();
  }

}
