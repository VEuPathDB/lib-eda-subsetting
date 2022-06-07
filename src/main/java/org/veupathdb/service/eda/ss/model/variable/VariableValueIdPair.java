package org.veupathdb.service.eda.ss.model.variable;

import java.util.Objects;

public class VariableValueIdPair<T> {
  public final String entityId;
  public final T value;

  public VariableValueIdPair(String entityId, T value) {
    this.entityId = entityId;
    this.value = value;
  }

  public String getEntityId() {
    return entityId;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariableValueIdPair<?> that = (VariableValueIdPair<?>) o;
    return Objects.equals(entityId, that.entityId) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, value);
  }

  @Override
  public String toString() {
    return "VariableValueIdPair{" +
        "entityId='" + entityId + '\'' +
        ", value=" + value +
        '}';
  }
}
