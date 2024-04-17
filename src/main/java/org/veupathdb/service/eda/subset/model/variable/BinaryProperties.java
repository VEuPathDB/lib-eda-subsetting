package org.veupathdb.service.eda.subset.model.variable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.veupathdb.service.eda.subset.model.variable.binary.EmptyBinaryProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = Utf8EncodingLengthProperties.class, name = "string"),
        @JsonSubTypes.Type(value = Utf8EncodingLengthProperties.class, name = "number"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "date"),
        @JsonSubTypes.Type(value = Utf8EncodingLengthProperties.class, name = "longitude"),
        @JsonSubTypes.Type(value = EmptyBinaryProperties.class, name = "integer") }
)
public abstract class BinaryProperties {
}
