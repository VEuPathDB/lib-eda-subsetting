package org.veupathdb.service.edass.model;

public class DistributionTuple {
  private String value;
  private Integer count;
  
  public DistributionTuple(String value, Integer count) {
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
