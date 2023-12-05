package org.veupathdb.service.eda.ss.model.variable;

public class Utf8EncodingLengthProperties extends BinaryProperties {
  private Integer maxLength;

  public Utf8EncodingLengthProperties() {
  }

  public Utf8EncodingLengthProperties(Integer maxLength) {
    this.maxLength = maxLength;
  }

  public Integer getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(Integer maxLength) {
    this.maxLength = maxLength;
  }
}
