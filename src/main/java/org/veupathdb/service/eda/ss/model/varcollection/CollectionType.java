package org.veupathdb.service.eda.ss.model.varcollection;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CollectionType {
  @JsonProperty("number")
  NUMBER("number"),

  @JsonProperty("date")
  DATE("date"),

  @JsonProperty("integer")
  INTEGER("integer");

  private String name;

  CollectionType(String name) {
    this.name = name;
  }
  public String getValue(){ return name; }
}

