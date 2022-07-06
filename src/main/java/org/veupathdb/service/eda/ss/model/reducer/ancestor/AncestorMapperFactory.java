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
  private BinaryFilesManager binaryFilesManager;

  public AncestorMapperFactory(BinaryFilesManager binaryFilesManager) {
    this.binaryFilesManager = binaryFilesManager;
  }

  public Iterator<Long> fromEntity(Iterator<Long> idStream, Study study, Entity from, Entity to) {
    if (from.getAncestorEntities().contains(to)) {
      Path path = binaryFilesManager.getAncestorFile(study, from, BinaryFilesManager.Operation.READ);
      final LongValueConverter longValueConverter = new LongValueConverter();
      final ListConverter<Long> listConverter = new ListConverter<>(longValueConverter, from.getAncestorEntities().size() + 1);
      final AncestorDeserializer ancestorDeserializer = new AncestorDeserializer(listConverter, from.getAncestorEntities().indexOf(to) + 1);
      try {
        return new DescendantCollapser(path, ancestorDeserializer, idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else if (to.getAncestorEntities().contains(from)) {
      Path path = binaryFilesManager.getAncestorFile(study, to, BinaryFilesManager.Operation.READ);
      final LongValueConverter longValueConverter = new LongValueConverter();
      final ListConverter<Long> listConverter = new ListConverter<>(longValueConverter, to.getAncestorEntities().size() + 1);
      final AncestorDeserializer ancestorDeserializer = new AncestorDeserializer(listConverter, to.getAncestorEntities().indexOf(from) + 1);
      try {
        return new AncestorExpander(path, ancestorDeserializer, idStream);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    throw new IllegalArgumentException();
  }
}
