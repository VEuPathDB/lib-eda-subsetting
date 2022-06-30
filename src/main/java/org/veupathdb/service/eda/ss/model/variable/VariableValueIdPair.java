package org.veupathdb.service.eda.ss.model.variable;

import java.util.Objects;

public class VariableValueIdPair<T> {
  public long idIndex;
  public T value;

  public VariableValueIdPair(long idIndex, T value) {
    this.idIndex = idIndex;
    this.value = value;
  }

  public long getIdIndex() {
    return idIndex;
  }

  public T getValue() {
    return value;
  }

  public void setIdIndex(long idIndex) {
    this.idIndex = idIndex;
  }

  public void setValue(T value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariableValueIdPair<?> that = (VariableValueIdPair<?>) o;
    return Objects.equals(idIndex, that.idIndex) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(idIndex, value);
  }

  @Override
  public String toString() {
    return "VariableValueIdPair{" +
        "index='" + idIndex + '\'' +
        ", value=" + value +
        '}';
  }
}
