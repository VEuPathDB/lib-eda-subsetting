package org.veupathdb.service.eda.ss.model.variable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.veupathdb.service.eda.ss.model.variable.binary.EmptyBinaryProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = StringVariable.StringBinaryProperties.class, name = "string"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "number"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "date"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "longitude"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "integer") }
)
public abstract class BinaryProperties {
}
