package org.veupathdb.service.eda.subset.model.reducer;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.filter.Filter;

import java.util.List;

/**
 * Contents of a node in the map-reduce data-flow tree used to produce a stream of ID indexes of output entity with
 * all relevant filters applied.
 */
public class DataFlowNodeContents {
  private final List<Filter> filters; // ID indexes of this Entity's "type" for filtered data streams.
  private final Entity entity;
  private final Study study;
  private final boolean includeUnfilteredStream;

  public DataFlowNodeContents(List<Filter> filters,
                              Entity entity,
                              Study study,
                              boolean includeUnfilteredStream) {
    this.filters = filters;
    this.entity = entity;
    this.study = study;
    this.includeUnfilteredStream = includeUnfilteredStream;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public boolean shouldIncludeUnfilteredStream() {
    return includeUnfilteredStream;
  }

  public Entity getEntity() {
    return entity;
  }

  public Study getStudy() {
    return study;
  }
}
