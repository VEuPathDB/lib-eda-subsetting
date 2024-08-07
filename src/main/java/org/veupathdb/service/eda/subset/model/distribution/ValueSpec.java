package org.veupathdb.service.eda.subset.model.distribution;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum ValueSpec {
  @JsonProperty("count")
  COUNT("count"),

  @JsonProperty("proportion")
  PROPORTION("proportion");

  private final String name;

  ValueSpec(String name) {
    this.name = name;
  }
  public String getValue(){ return name; }
}

