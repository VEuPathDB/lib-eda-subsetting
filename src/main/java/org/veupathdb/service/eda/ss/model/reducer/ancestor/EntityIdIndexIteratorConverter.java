package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.reducer.BinaryValuesStreamer;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Converts a stream of ID indexes of one entity type to ID indexes of either an ancestor of a descendant.
 */
public class EntityIdIndexIteratorConverter {
  private static final Logger LOG = LogManager.getLogger(EntityIdIndexIteratorConverter.class);

  private static final LongValueConverter LONG_VALUE_CONVERTER = new LongValueConverter();

  private BinaryValuesStreamer binaryValuesStreamer;

  public EntityIdIndexIteratorConverter(BinaryValuesStreamer binaryValuesStreamer) {
    this.binaryValuesStreamer = binaryValuesStreamer;
  }

  /**
   * @param idStream Stream of ID indexes corresponding to from {@code Entity}
   * @param study containing entities, used to locate relevant binary files
   * @param from Source of entity type to map IDs from
   * @param to Destination entity type to map IDs to
   * @return a stream of idIndexes corresponding to {@code Entity}
   */
  public CloseableIterator<Long> fromEntity(CloseableIterator<Long> idStream, Study study, Entity from, Entity to) {
    LOG.info("Mapping IDs from " + from.getDisplayName() + " to " + to.getDisplayName());
    // Check who is an ancestor of whom, this determines how we will convert the input idStream.
    if (from.getAncestorEntities().contains(to)) {
      try {
        // If "to" is an ancestor of "from", open "from"'s ancestor file.
        return new DescendantCollapser(binaryValuesStreamer.streamAncestorIds(from, study, from.getAncestorEntities().indexOf(to) + 1), idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (to.getAncestorEntities().contains(from)) {
      // If "from" is an ancestor of "to", open "to"'s ancestor file.
      try {
        return new AncestorExpander(binaryValuesStreamer.streamAncestorIds(to, study, to.getAncestorEntities().indexOf(from) + 1), idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalArgumentException("Entities must be related to one another to construct and ancestor mapper.");
  }
}
