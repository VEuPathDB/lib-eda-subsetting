package org.veupathdb.service.eda.subset.model.reducer;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.BinaryProperties;

import java.util.Optional;

/**
 * Simple functional interface for providing binary metadata describing variables.
 */
public interface BinaryMetadataProvider {
    /**
     *
     * @param studyAbbrev Internal study abbreviation that entity and variable belong to.
     * @param entity Entity that the variable belongs to.
     * @param variableId Identifier of the variable.
     * @return Empty if binary properties cannot be provided. Otherwise, returns the metadata associated with the variable.
     */
    Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId);
}
