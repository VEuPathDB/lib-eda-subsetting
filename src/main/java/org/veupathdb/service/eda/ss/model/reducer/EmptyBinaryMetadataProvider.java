package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.variable.BinaryProperties;

import java.util.Optional;

public class EmptyBinaryMetadataProvider implements BinaryMetadataProvider {

    private static final Logger LOG = LogManager.getLogger(EmptyBinaryMetadataProvider.class);

    @Override
    public Optional<BinaryProperties> getBinaryProperties(String studyAbbrev, Entity entity, String variableId) {
        LOG.warn("Why are we inside an EmptyBinaryMetadataProvider???");
        return Optional.empty();
    }
}
