package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;

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

  public boolean isCompatibleWithCollectionShape(VariableDataShape collectionShape, List<String> collectionVocab) {
    if (this == BINARY || collectionShape == BINARY) {
      return collectionVocab.size() <= 2;
    }
    
  }
}
