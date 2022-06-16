package org.veupathdb.service.eda.ss.model.variable.binary;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.List;

/**
 * Binary deserializer that knows how to deserialize records with an entity identifier and all of that entity's
 * ancestors. Rather than returning all ancestors, it only returns a single ancestor specified by the index
 * in the constructor.
 */
public class AncestorDeserializer implements BinaryDeserializer<VariableValueIdPair<Long>> {
  private ListConverter<Long> listConverter;
  private int ancestorIndex;

  /**
   * Creates a new instance to deserialize ancestor files, extracting only the entity ID and the ID of the ancestor
   * of interest specified by the {@param ancestorIndex}.
   *
   * @param listConverter Tuple serializer used to deserialize all ancestors.
   * @param ancestorIndex Index of ancestor to be returned.
   */
  public AncestorDeserializer(ListConverter<Long> listConverter, int ancestorIndex) {
    this.listConverter = listConverter;
    this.ancestorIndex = ancestorIndex;
  }

  @Override
  public VariableValueIdPair<Long> fromBytes(byte[] bytes) {
    List<Long> ancestors = listConverter.fromBytes(bytes);
    return new VariableValueIdPair<>(ancestors.get(0), ancestors.get(ancestorIndex));
  }

  @Override
  public int numBytes() {
    return listConverter.numBytes() * listConverter.getSize();
  }
}
