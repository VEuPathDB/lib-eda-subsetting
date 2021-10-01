package org.veupathdb.service.eda.ss.model.filter;

import static org.gusdb.fgputil.FormatUtil.NL;

import java.util.Objects;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;

public abstract class Filter {

  protected Entity entity;

  public Filter(Entity entity) {
    Objects.nonNull(entity);
    this.entity = entity;
  }

  public abstract String getSql();

  /*
   * Get SQL to perform the filter. Include ancestor IDs.
   */
  protected String getSingleFilterCommonSqlWithAncestors() {

    // join to ancestors table to get ancestor ID

    return "  SELECT " + entity.getAllPksSelectList("a") + NL
        + "  FROM " + Resources.getAppDbSchema() + entity.getTallTableName() + " t, " + Resources.getAppDbSchema() + entity.getAncestorsTableName() + " a" + NL
        + "  WHERE t." + entity.getPKColName() + " = a." + entity.getPKColName();
  }

  protected String getSingleFilterCommonSqlNoAncestors() {

    return "  SELECT " + entity.getPKColName() + NL
        + "  FROM " + Resources.getAppDbSchema() + entity.getTallTableName() + NL
        + "  WHERE 1 = 1 --no-op where clause for code generation simplicity";
  }

  public Entity getEntity() {
    return entity;
  }
}
