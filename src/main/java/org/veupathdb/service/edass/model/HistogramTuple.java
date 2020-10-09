package org.veupathdb.service.edass.model;

public class HistogramTuple {
  private String value;
  private Integer count;
  
  public HistogramTuple(String value, Integer count) {
    this.value = value;
    this.count = count;
  }

  public String getValue() {
    return value;
  }

  public Integer getCount() {
    return count;
  }

}
