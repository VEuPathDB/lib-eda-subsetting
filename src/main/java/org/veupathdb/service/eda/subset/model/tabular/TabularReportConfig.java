package org.veupathdb.service.eda.subset.model.tabular;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TabularReportConfig {
  private static final Logger LOG = LogManager.getLogger(TabularReportConfig.class);

  private List<SortSpecEntry> _sorting = new ArrayList<>();
  private Optional<Long> _numRows = Optional.empty();
  private Long _offset = 0L;
  private TabularHeaderFormat _headerFormat = TabularHeaderFormat.STANDARD;
  private boolean _trimTimeFromDateVars = false;
  private DataSourceType _dataSourceType = DataSourceType.UNSPECIFIED;

  /**
   * Whether this configuration contains paging or sorting (paging always requires sorting)
   *
   * @return true if paging or sorting config is not the default, else false
   */
  public boolean requiresSorting() {
    return !_sorting.isEmpty() || _numRows.isPresent() || _offset != 0L;
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

  public TabularHeaderFormat getHeaderFormat() {
    return _headerFormat;
  }

  public boolean getTrimTimeFromDateVars() {
    return _trimTimeFromDateVars;
  }

  public DataSourceType getDataSourceType() {
    return _dataSourceType;
  }

  public void setNumRows(Optional<Long> numRows) {
    _numRows = numRows;
  }

  public void setOffset(Long offset) {
    _offset = offset;
  }

  public void setSorting(List<SortSpecEntry> sorting) {
    _sorting = sorting;
  }

  public void setHeaderFormat(TabularHeaderFormat headerFormat) {
    _headerFormat = headerFormat;
  }

  public void setTrimTimeFromDateVars(boolean trimTimeFromDateVars) {
    _trimTimeFromDateVars = trimTimeFromDateVars;
  }

  public void setDataSourceType(DataSourceType dataSourceType) {
    _dataSourceType = dataSourceType;
  }
}
