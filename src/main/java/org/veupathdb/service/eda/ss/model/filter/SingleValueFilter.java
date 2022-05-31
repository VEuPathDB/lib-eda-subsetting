package org.veupathdb.service.eda.ss.model.filter;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import java.util.function.Predicate;

import static org.gusdb.fgputil.FormatUtil.NL;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.AttributeValue.Columns.TT_VARIABLE_ID_COL_NAME;

/**
 *
 * @param <T> VariableType that this filter is applied to.
 * @param <U> Encoded representation of variable values.
 */
public abstract class SingleValueFilter<T extends VariableWithValues, U> extends Filter {

  protected T _variable;

  public SingleValueFilter(String appDbSchema, Entity entity, T variable) {
    super(appDbSchema, entity);
    entity.getVariable(variable.getId()).orElseThrow(
        () -> new RuntimeException("Entity " + entity.getId() + " does not contain variable " + variable.getId()));
    _variable = variable;
  }

  public T getVariable() {
    return _variable;
  }

  @Override
  public String getSql() {
    return _entity.getAncestorPkColNames().isEmpty() ? getSqlNoAncestors() : getSqlWithAncestors();
  }

  /**
   * subclasses provide an AND clause to find rows that match their filter
   */
  public abstract String getFilteringAndClausesSql();

  public abstract Predicate<U> getPredicate();

  // join to ancestors table to get ancestor IDs
  String getSqlWithAncestors() {

    return getSingleFilterCommonSqlWithAncestors() + NL
        + "  AND " + TT_VARIABLE_ID_COL_NAME + " = '" + _variable.getId() + "'" + NL
        + getFilteringAndClausesSql();
  }

  String getSqlNoAncestors() {

    return getSingleFilterCommonSqlWithAncestors() + NL
        + "  AND " + TT_VARIABLE_ID_COL_NAME + " = '" + _variable.getId() + "'" + NL
        + getFilteringAndClausesSql();
  }

}
