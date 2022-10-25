package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.binary.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Converts a stream of ID indexes of one entity type to ID indexes of either an ancestor of a descendant.
 */
public class EntityIdIndexIteratorConverter {
  private static final LongValueConverter LONG_VALUE_CONVERTER = new LongValueConverter();

  private BinaryFilesManager binaryFilesManager;

  public EntityIdIndexIteratorConverter(BinaryFilesManager binaryFilesManager) {
    this.binaryFilesManager = binaryFilesManager;
  }

  /**
   * @param idStream Stream of ID indexes corresponding to from {@code Entity}
   * @param study containing entities, used to locate relevant binary files
   * @param from Source of entity type to map IDs from
   * @param to Destination entity type to map IDs to
   * @return a stream of idIndexes corresponding to {@code Entity}
   */
  public CloseableIterator<Long> fromEntity(CloseableIterator<Long> idStream, Study study, Entity from, Entity to) {
    // Check who is an ancestor of whom, this determines how we will convert the input idStream.
    if (from.getAncestorEntities().contains(to)) {
      // If "to" is an ancestor of "from", open "from"'s ancestor file.
      Path path = binaryFilesManager.getAncestorFile(study, from, BinaryFilesManager.Operation.READ);
      final ArrayConverter<Long> listConverter = new ArrayConverter<>(LONG_VALUE_CONVERTER, from.getAncestorEntities().size() + 1, Long.class);
      final AncestorDeserializer ancestorDeserializer = new AncestorDeserializer(listConverter, from.getAncestorEntities().indexOf(to) + 1);
      try {
        return new DescendantCollapser(path, ancestorDeserializer, idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (to.getAncestorEntities().contains(from)) {
      // If "from" is an ancestor of "to", open "to"'s ancestor file.
      Path path = binaryFilesManager.getAncestorFile(study, to, BinaryFilesManager.Operation.READ);
      final ArrayConverter<Long> listConverter = new ArrayConverter<>(LONG_VALUE_CONVERTER, to.getAncestorEntities().size() + 1, Long.class);
      final AncestorDeserializer ancestorDeserializer = new AncestorDeserializer(listConverter, to.getAncestorEntities().indexOf(from) + 1);
      try {
        return new AncestorExpander(path, ancestorDeserializer, idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalArgumentException("Entities must be related to one another to construct and ancestor mapper.");
  }
}
