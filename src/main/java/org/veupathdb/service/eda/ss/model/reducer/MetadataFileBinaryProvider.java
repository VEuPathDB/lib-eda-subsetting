package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.BinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;

import java.util.Optional;

/**
 * Implementation of {@link BinaryMetadataProvider} that reads metadata from a meta.json file. This is designed to be
 * used by a consumer of the generated binary files.
 */
public class MetadataFileBinaryProvider implements BinaryMetadataProvider {
    private BinaryFilesManager binaryFilesManager;

    public MetadataFileBinaryProvider(BinaryFilesManager binaryFilesManager) {
        this.binaryFilesManager = binaryFilesManager;
    }

    @Override
    public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
        if (!binaryFilesManager.studyHasFiles(studyAbbrev)) {
            return Optional.empty();
        }
        Optional<BinaryFilesManager.Metadata> metadata = binaryFilesManager.readMetadata(studyAbbrev, entity);
        Optional<BinaryFilesManager.VariableMeta> varMetadata = metadata.flatMap(meta -> meta.getVariableMetadata().stream()
            .filter(varMeta -> varMeta.getVariableId().equals(variableId))
            .findFirst());
        return varMetadata.map(varMeta -> varMeta.getProperties());
    }
}
