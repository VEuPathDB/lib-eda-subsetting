package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import java.util.List;

public interface AncestorMapper {
  // TODO: The output of this needs to be sort/uniqued
  List<Integer> mapFromId(int id);
}
