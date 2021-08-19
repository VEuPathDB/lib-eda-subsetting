package org.veupathdb.service.eda.ss.model;

public class Variable {

  private final String providerLabel;
  private final String id;
  private final Entity entity;
  private final VariableType type;
  private final VariableDataShape dataShape;
  private final VariableDisplayType displayType;
  private final String displayName;
  private final String parentId;
  private final String definition;
  private final boolean hasValues;

  private final Integer displayOrder;

  private final static String CONT_STR = "continuous";
  private final static String CAT_STR = "categorical";
  private final static String ORD_STR = "ordinal";
  private final static String TWO_STR = "binary";

  public enum VariableDataShape {
    CONTINUOUS(CONT_STR),
    CATEGORICAL(CAT_STR),
    ORDINAL(ORD_STR),
    BINARY(TWO_STR);

    private final String name;

    VariableDataShape(String name) {
      this.name = name;
    }

    public static VariableDataShape fromString(String shapeString) {

      VariableDataShape v;

      switch (shapeString) {
        case CONT_STR -> v = CONTINUOUS;
        case CAT_STR -> v = CATEGORICAL;
        case ORD_STR -> v = ORDINAL;
        case TWO_STR -> v = BINARY;
        default -> throw new RuntimeException("Unrecognized data shape: " + shapeString);
      }
      return v;
    }

    public String getName() { return name;}
  }

  public enum VariableDisplayType {
    DEFAULT("default"),
    HIDDEN("hidden"),
    MULTIFILTER("multiFilter");

    String type;

    VariableDisplayType(String type) {
      this.type = type;
    }

    public static VariableDisplayType fromString(String displayType) {

      VariableDisplayType t;

      switch (displayType.toLowerCase()) {
        case "default" -> t = DEFAULT;
        case "multifilter" -> t = MULTIFILTER; 
        case "hidden" -> t = HIDDEN;
        default -> throw new RuntimeException("Unrecognized variable display type: " + displayType);
      }
      return t;
    }

    public String getType() { return type; }
  }

  /*
  Construct a variable that has values
   */
  public Variable(String providerLabel, String id, Entity entity, VariableType type, VariableDataShape shape, VariableDisplayType displayType, String displayName, Integer displayOrder, String parentId, String definition) {
	  this(providerLabel, id, entity, true, type, shape, displayType, displayName, displayOrder, parentId, definition);
  }

  /*
  Construct a variable that does not have values
   */
  public Variable(String providerLabel, String id, Entity entity, VariableDisplayType displayType, String displayName, Integer displayOrder, String parentId) {
	  this(providerLabel, id, entity, false, null, null, displayType, displayName, displayOrder, parentId, null);
  }

  private Variable(String providerLabel, String id, Entity entity, boolean hasValues, VariableType type, VariableDataShape shape, VariableDisplayType displayType, String displayName, Integer displayOrder, String parentId, String definition) {
	    this.providerLabel = providerLabel;
	    this.id = id;
	    this.entity = entity;
	    this.type = type;
	    this.dataShape = shape;
	    this.displayType = displayType;
	    this.hasValues = hasValues;
	    this.displayName = displayName;
	    this.displayOrder = displayOrder;
	    this.parentId = parentId;
	    this.definition= definition;
	  }
  
  public String getProviderLabel() {
    return providerLabel;
  }

  public String getId() {
    return id;
  }

  public boolean getHasValues() {
	return hasValues;
  }

  public VariableDisplayType getDisplayType() {
	return displayType;
  }

  public String getEntityId() {
    return entity.getId();
  } 
  
  public Entity getEntity() {
    return entity;
  } 
  
  public VariableDataShape getDataShape() {
    return dataShape;
  }
  
  public String getDisplayName() {
    return displayName;
  }

  public String getParentId() {
    return parentId;
  }

  public VariableType getType() {
    return type;
  }
  
  public String getDefinition() {
		return definition;
	}

	public Integer getDisplayOrder() {
		return displayOrder;
	}

}
