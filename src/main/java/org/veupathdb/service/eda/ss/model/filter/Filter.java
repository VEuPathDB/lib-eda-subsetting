package org.veupathdb.service.eda.ss.model.filter;

import static org.gusdb.fgputil.FormatUtil.NL;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.db.DB;
import org.veupathdb.service.eda.ss.model.reducer.BinaryValuesStreamer;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

public abstract class Filter {

  protected final Entity _entity;
  protected final String _appDbSchema;

  public Filter(String appDbSchema, Entity entity) {
    Objects.requireNonNull(entity);
    _entity = entity;
    _appDbSchema = appDbSchema;
  }

  public abstract String getSql();

  public abstract CloseableIterator<Long> streamFilteredIds(BinaryValuesStreamer binaryValuesStreamer, Study study);

  public abstract List<VariableWithValues> getAllVariables();

  /*
   * Get SQL to perform the filter. Include ancestor IDs.
   */
  protected String getSingleFilterCommonSqlWithAncestors() {

    // join to ancestors table to get ancestor ID

    return "  SELECT " + _entity.getAllPksSelectList("a") + NL
        + "  FROM " + _appDbSchema + DB.Tables.AttributeValue.NAME(_entity) + " t, " + _appDbSchema + DB.Tables.Ancestors.NAME(_entity) + " a" + NL
        + "  WHERE t." + _entity.getPKColName() + " = a." + _entity.getPKColName();
  }

  protected String getSingleFilterCommonSqlNoAncestors() {

    return "  SELECT " + _entity.getPKColName() + NL
        + "  FROM " + _appDbSchema + DB.Tables.AttributeValue.NAME(_entity) + NL
        + "  WHERE 1 = 1 --no-op where clause for code generation simplicity";
  }

  public Entity getEntity() {
    return _entity;
  }

  public boolean filtersOnVariable(VariableWithValues variable) {
    if (!_entity.getId().equals(variable.getEntity().getId())) {
      return false;
    }
    return getAllVariables().stream().anyMatch(var -> var.getId().equals(variable.getId()));
  }
}
