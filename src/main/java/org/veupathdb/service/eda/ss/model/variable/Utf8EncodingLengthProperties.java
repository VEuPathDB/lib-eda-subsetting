package org.veupathdb.service.eda.ss.model.variable;

public class Utf8EncodingLengthProperties extends BinaryProperties {
  private int maxLength;

  public Utf8EncodingLengthProperties() {
  }

  public Utf8EncodingLengthProperties(int maxLength) {
    this.maxLength = maxLength;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public void setMaxLength(int maxLength) {
    this.maxLength = maxLength;
  }
}
