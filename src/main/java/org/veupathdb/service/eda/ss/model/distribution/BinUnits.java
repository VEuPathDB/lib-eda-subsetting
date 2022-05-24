package org.veupathdb.service.eda.ss.model.distribution;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum BinUnits {
  @JsonProperty("day")
  DAY("day"),

  @JsonProperty("week")
  WEEK("week"),

  @JsonProperty("month")
  MONTH("month"),

  @JsonProperty("year")
  YEAR("year");

  private String name;

  BinUnits(String name) {
    this.name = name;
  }
  public String getValue(){ return name; }
}
