package org.veupathdb.service.eda.ss.model.variable;

import java.util.Objects;

public class VariableValue<T> {
  public int entityId;
  public T value;

  public VariableValue(int entityId, T value) {
    this.entityId = entityId;
    this.value = value;
  }

  public int getEntityId() {
    return entityId;
  }

  public T getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariableValue<?> that = (VariableValue<?>) o;
    return entityId == that.entityId && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, value);
  }
}
