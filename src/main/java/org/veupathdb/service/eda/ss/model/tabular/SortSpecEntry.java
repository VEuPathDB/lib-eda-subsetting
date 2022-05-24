package org.veupathdb.service.eda.ss.model.tabular;

import org.gusdb.fgputil.SortDirection;

public class SortSpecEntry {

  private String _key;
  private SortDirection _direction;

  public String getKey() {
    return _key;
  }

  public SortSpecEntry setKey(String key) {
    _key = key;
    return this;
  }

  public SortDirection getDirection() {
    return _direction;
  }

  public SortSpecEntry setDirection(SortDirection direction) {
    _direction = direction;
    return this;
  }
}

