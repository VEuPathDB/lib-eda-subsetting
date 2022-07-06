package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.variable.binary.AncestorDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.BinaryFilesManager;
import org.veupathdb.service.eda.ss.model.variable.binary.ListConverter;
import org.veupathdb.service.eda.ss.model.variable.binary.LongValueConverter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class AncestorMapperFactory {
  private static final LongValueConverter LONG_VALUE_CONVERTER = new LongValueConverter();

  private BinaryFilesManager binaryFilesManager;

  public AncestorMapperFactory(BinaryFilesManager binaryFilesManager) {
    this.binaryFilesManager = binaryFilesManager;
  }

  /**
   * @param idStream Stream of idIndexes corresponding to from {@code Entity}
   * @param study containing entities, used to locate relevant binary files
   * @param from Source of entity type to map IDs from
   * @param to Destination entity type to map IDs to
   * @return a stream of idIndexes corresponding to {@code Entity}
   */
  public Iterator<Long> fromEntity(Iterator<Long> idStream, Study study, Entity from, Entity to) {
    if (from.getAncestorEntities().contains(to)) {
      Path path = binaryFilesManager.getAncestorFile(study, from, BinaryFilesManager.Operation.READ);
      final ListConverter<Long> listConverter = new ListConverter<>(LONG_VALUE_CONVERTER, from.getAncestorEntities().size() + 1);
      final AncestorDeserializer ancestorDeserializer = new AncestorDeserializer(listConverter, from.getAncestorEntities().indexOf(to) + 1);
      try {
        return new DescendantCollapser(path, ancestorDeserializer, idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (to.getAncestorEntities().contains(from)) {
      Path path = binaryFilesManager.getAncestorFile(study, to, BinaryFilesManager.Operation.READ);
      final ListConverter<Long> listConverter = new ListConverter<>(LONG_VALUE_CONVERTER, to.getAncestorEntities().size() + 1);
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
