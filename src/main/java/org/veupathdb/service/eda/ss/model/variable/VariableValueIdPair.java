package org.veupathdb.service.eda.ss.model.variable;

import java.util.Objects;

public class VariableValueIdPair<T> {
  public final Long index;
  public final T value;

  public VariableValueIdPair(Long index, T value) {
    this.index = index;
    this.value = value;
  }

  public Long getIndex() {
    return index;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariableValueIdPair<?> that = (VariableValueIdPair<?>) o;
    return Objects.equals(index, that.index) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, value);
  }

  @Override
  public String toString() {
    return "VariableValueIdPair{" +
        "index='" + index + '\'' +
        ", value=" + value +
        '}';
  }
}
