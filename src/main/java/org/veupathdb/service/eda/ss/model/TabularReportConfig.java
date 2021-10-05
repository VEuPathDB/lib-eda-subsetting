package org.veupathdb.service.eda.ss.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.ws.rs.BadRequestException;
import org.veupathdb.service.eda.generated.model.APITabularReportConfig;
import org.veupathdb.service.eda.generated.model.SortSpecEntry;

public class TabularReportConfig {

  private List<SortSpecEntry> _sorting = new ArrayList<>();
  private Optional<Long> _numRows = Optional.empty();
  private Long _offset = 0L;

  public TabularReportConfig(Entity entity, APITabularReportConfig apiConfig) {
    if (apiConfig == null) return;
    if (apiConfig.getPaging() != null) {
      _numRows = Optional.of(apiConfig.getPaging().getNumRows());
      _offset = apiConfig.getPaging().getOffset();
      if (_offset < 0)
        throw new BadRequestException("In paging config, offset must a non-negative integer.");
      if (_numRows.get() <= 0)
        throw new BadRequestException("In paging config, numRows must a positive integer.");
    }
    List<SortSpecEntry> sorting = apiConfig.getSorting();
    if (sorting != null && !sorting.isEmpty()) {
      for (SortSpecEntry entry : sorting) {
        entity.getVariableOrThrow(entry.getKey());
      }
      _sorting = sorting;
    }
  }

  public boolean requiresPaging() {
    return !_sorting.isEmpty() || !_numRows.isEmpty() || _offset != 0L;
  }

  public List<SortSpecEntry> getSorting() {
    return _sorting;
  }

  public Optional<Long> getNumRows() {
    return _numRows;
  }

  public Long getOffset() {
    return _offset;
  }

}
