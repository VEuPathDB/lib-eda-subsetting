package org.veupathdb.service.eda.subset.model.reducer;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.BinaryProperties;
import org.veupathdb.service.eda.subset.model.variable.binary.BinaryFilesManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * Implementation of {@link BinaryMetadataProvider} that reads metadata from a
 * <code>meta.json</code> file. This is designed to be used by a consumer of the
 * generated binary files.
 */
public class MetadataFileBinaryProvider implements BinaryMetadataProvider {

  private static final Logger LOG = LogManager.getLogger(MetadataFileBinaryProvider.class);

  private final BinaryFilesManager binaryFilesManager;

  public MetadataFileBinaryProvider(BinaryFilesManager binaryFilesManager) {
    this.binaryFilesManager = binaryFilesManager;
  }

  @Override
  public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
    if (!binaryFilesManager.studyHasFiles(studyAbbrev)) {
      LOG.error("Could not find files for study '{}' (using entity '{}' and variable '{}') while trying to find binary properties for a variable.", studyAbbrev, entity.getId(), variableId);
      return Optional.empty();
    }

    Optional<BinaryFilesManager.Metadata> metadata = binaryFilesManager.readMetadata(studyAbbrev, entity);
    if (metadata.isEmpty()) {
      LOG.error("Unable to read metadata file for study '{}' (using entity '{}') even though files are present.", studyAbbrev, entity.getId());
      return Optional.empty();
    }

    Optional<BinaryFilesManager.VariableMeta> varMetadata = metadata.flatMap(meta -> meta.getVariableMetadata().stream()
      .filter(varMeta -> varMeta.getVariableId().equals(variableId))
      .findFirst());
    if (varMetadata.isEmpty()) {
      LOG.error("Unable to find metadata object for study '{}', entity '{}', and variable '{}' in metadata file.", studyAbbrev, entity.getId(), variableId);
    }

    return varMetadata.map(BinaryFilesManager.VariableMeta::getProperties);
  }
}
