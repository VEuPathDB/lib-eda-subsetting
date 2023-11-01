package org.veupathdb.service.eda.ss.model.variable;

import java.util.List;

import org.veupathdb.service.eda.ss.model.Entity;

public abstract class Variable {

  public static class Properties {

    public final String providerLabel;
    public final String id;
    public final Entity entity;
    public final VariableDisplayType displayType;
    public final String displayName;
    public final Long displayOrder;
    public final String parentId;
    public final String definition;
    public final List<String> hideFrom;

    public Properties(String providerLabel, String id, Entity entity,
                      VariableDisplayType displayType, String displayName,
                      Long displayOrder, String parentId, String definition,
                      List<String> hideFrom) {
      this.providerLabel = providerLabel;
      this.id = id;
      this.entity = entity;
      this.displayType = displayType;
      this.displayName = displayName;
      this.displayOrder = displayOrder;
      this.parentId = parentId;
      this.definition = definition;
      this.hideFrom = hideFrom;
    }
  }

  private final Properties _properties;

  public abstract boolean hasValues();

  protected Variable(Properties properties) {
    _properties = properties;
  }

  public String getProviderLabel() {
    return _properties.providerLabel;
  }

  public String getId() {
    return _properties.id;
  }

  public VariableDisplayType getDisplayType() {
    return _properties.displayType;
  }

  public String getEntityId() {
    return _properties.entity.getId();
  }

  public Entity getEntity() {
    return _properties.entity;
  }

  public String getDisplayName() {
    return _properties.displayName;
  }

  public String getParentId() {
    return _properties.parentId;
  }

  public Long getDisplayOrder() {
    return _properties.displayOrder;
  }

  public String getDefinition() {
    return _properties.definition;
  }
  
  public List<String> getHideFrom() {
    return _properties.hideFrom;
  }

  public String getDownloadColHeader() {
    return getDisplayName() + " [" + getId() + "]";
  }

  public String getDotNotation() {
    return getEntityId() + "." + getId();
  }

  public boolean hasGeographicData() {
    return getDisplayType() == VariableDisplayType.LATITUDE
        || getDisplayType() == VariableDisplayType.LONGITUDE
        || getDisplayType() == VariableDisplayType.GEOAGGREGATOR;
  }

}
