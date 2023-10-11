package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;
import java.util.Set;

public enum VariableDataShape {
  CONTINUOUS("continuous"),
  CATEGORICAL("categorical"),
  ORDINAL("ordinal"),
  BINARY("binary");

  private final String _name;

  VariableDataShape(String name) {
    _name = name;
  }

  public static VariableDataShape fromString(String shapeString) {
    for (VariableDataShape shape : values()) {
      if (shape._name.equals(shapeString)) {
        return shape;
      }
    }
    throw new RuntimeException("Unrecognized data shape: " + shapeString);
  }

  /**
   * Check if this data shape instance is allowed to be a member of a collection with the given collectionShape
   * and collection vocabulary.
   *
   * If the shapes are the same, they should always be compatible. Binary variables are compatible with a binary
   * collection if they have 2 or fewer vocab values.
   */
  public boolean isCompatibleWithCollectionShape(VariableDataShape collectionShape,
                                                 List<String> variableVocab,
                                                 Set<String> collectionVocab) {
    if (this == BINARY && collectionShape == CATEGORICAL) {
      // Binary is compatible as a variable data shape with a cat collection.
      return true;
    }
    if (collectionShape == BINARY) {
      // Binary collections can have CATEGORICAL vars if they have 0, 1 or 2 distinct values.
      return variableVocab.size() <= 2;
    }
    // Otherwise, collection type should be the same as variable type.
    return collectionShape._name.equals(this._name);
  }
}
