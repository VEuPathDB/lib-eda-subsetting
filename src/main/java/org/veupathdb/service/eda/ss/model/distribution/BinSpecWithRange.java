package org.veupathdb.service.eda.ss.model.distribution;

public class BinSpecWithRange {

  private Object _displayRangeMin;
  private Object _displayRangeMax;
  private Number _binWidth;
  private BinUnits _binUnits;

  public Object get_displayRangeMin() {
    return _displayRangeMin;
  }

  public BinSpecWithRange set_displayRangeMin(Object displayRangeMin) {
    _displayRangeMin = _displayRangeMin;
    return this;
  }

  public Object getDisplayRangeMax() {
    return _displayRangeMax;
  }

  public BinSpecWithRange setDisplayRangeMax(Object displayRangeMax) {
    _displayRangeMax = displayRangeMax;
    return this;
  }

  public Number getBinWidth() {
    return _binWidth;
  }

  public BinSpecWithRange setBinWidth(Number binWidth) {
    _binWidth = binWidth;
    return this;
  }

  public BinUnits getBinUnits() {
    return _binUnits;
  }

  public BinSpecWithRange setBinUnits(BinUnits binUnits) {
    _binUnits = binUnits;
    return this;
  }
}

