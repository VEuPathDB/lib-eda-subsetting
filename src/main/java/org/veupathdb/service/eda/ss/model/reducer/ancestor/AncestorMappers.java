package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.Entity;

import java.util.Iterator;

public class AncestorMappers {

  public static Iterator<Long> fromEntity(Entity from, Entity to) {
    if (from.getAncestorEntities().contains(to)) {

      return null;
    } else if (to.getAncestorEntities().contains(from)) {
      // Return Expander
      return null;
    }
    throw new IllegalArgumentException();
  }
}
