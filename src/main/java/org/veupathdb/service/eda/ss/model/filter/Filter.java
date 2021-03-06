package org.veupathdb.service.eda.ss.model.filter;

import static org.gusdb.fgputil.FormatUtil.NL;

import java.util.Objects;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.db.DB;

public abstract class Filter {

  protected final Entity _entity;
  protected final String _appDbSchema;

  public Filter(String appDbSchema, Entity entity) {
    Objects.requireNonNull(entity);
    _entity = entity;
    _appDbSchema = appDbSchema;
  }

  public abstract String getSql();

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
}
