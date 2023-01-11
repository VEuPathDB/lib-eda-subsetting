package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.BinaryProperties;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;

import java.util.Optional;

public class MetadataFileBinaryProvider implements BinaryMetadataProvider {
    private BinaryFilesManager binaryFilesManager;

    public MetadataFileBinaryProvider(BinaryFilesManager binaryFilesManager) {
        this.binaryFilesManager = binaryFilesManager;
    }

    @Override
    public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
        Optional<BinaryFilesManager.Metadata> metadata = binaryFilesManager.readMetadata(studyAbbrev, entity);
        Optional<BinaryFilesManager.VariableMeta> varMetadata = metadata.flatMap(meta -> meta.getVariableMetadata().stream()
                .filter(varMeta -> varMeta.getVariableId().equals(variableId))
                .findFirst());
        return varMetadata.map(varMeta -> varMeta.getProperties());
    }
}
