package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.Entity;

public class AncestorMappers {

  public static AncestorMapper fromEntity(Entity from, Entity to) {
    if (from.getAncestorEntities().contains(to)) {
      // Return Collapser
      return null;
    } else if (to.getAncestorEntities().contains(from)) {
      // Return Expander
      return null;
    }
    throw new IllegalArgumentException();
  }
}
