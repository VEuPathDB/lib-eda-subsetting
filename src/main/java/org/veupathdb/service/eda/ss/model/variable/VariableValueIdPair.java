package org.veupathdb.service.eda.ss.model.variable;

import java.util.Objects;

public class VariableValueIdPair<T> {
  public final Long idIndex;
  public final T value;

  public VariableValueIdPair(Long idIndex, T value) {
    this.idIndex = idIndex;
    this.value = value;
  }

  public Long getIdIndex() {
    return idIndex;
  }

  public T getValue() {
    return value;
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
