package org.veupathdb.service.eda.ss.model.variable;

import org.veupathdb.service.eda.generated.model.APIVariableDataShape;

public enum VariableDataShape {
  CONTINUOUS("continuous", APIVariableDataShape.CONTINUOUS),
  CATEGORICAL("categorical", APIVariableDataShape.CATEGORICAL),
  ORDINAL("ordinal", APIVariableDataShape.ORDINAL),
  BINARY("binary", APIVariableDataShape.BINARY);

  private final String _name;
  private final APIVariableDataShape _apiDataShape;

  VariableDataShape(String name, APIVariableDataShape apiDataShape) {
    _name = name;
    _apiDataShape = apiDataShape;
  }

  public static VariableDataShape fromString(String shapeString) {
    for (VariableDataShape shape : values()) {
      if (shape._name.equals(shapeString)) {
        return shape;
      }
    }
    throw new RuntimeException("Unrecognized data shape: " + shapeString);
  }

  public APIVariableDataShape toApiShape() {
    return _apiDataShape;
  }
}
