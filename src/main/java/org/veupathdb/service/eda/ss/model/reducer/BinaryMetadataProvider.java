package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.BinaryProperties;

import java.util.Optional;

public interface BinaryMetadataProvider {
    Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId);
}
