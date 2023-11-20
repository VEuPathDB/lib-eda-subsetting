package org.veupathdb.service.eda.subset.model.reducer;

import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.variable.BinaryProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

public class EmptyBinaryMetadataProvider implements BinaryMetadataProvider {

    private static final Logger LOG = LogManager.getLogger(EmptyBinaryMetadataProvider.class);

    @Override
    public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
        LOG.warn("Why are we inside an EmptyBinaryMetadataProvider???");
        return Optional.empty();
    }
}
