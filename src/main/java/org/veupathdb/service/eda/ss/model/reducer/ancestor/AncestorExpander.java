package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.reducer.FilteredValueFile;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.AncestorFileDeserializer;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Function;

public class AncestorExpander implements Iterator<Long> {
  private final Iterator<VariableValueIdPair<Long>> ancestorMappingStream;
  private final Iterator<Long> ancestorStream;
  private VariableValueIdPair<Long> currentAncestorMapping;
  private Long currentAncestor;
  private boolean hasStarted = false;

  public AncestorExpander(Path ancestorFilePath,
                          AncestorFileDeserializer deserializer,
                          Iterator<Long> ancestorStream) throws IOException {
    this.ancestorMappingStream = new FilteredValueFile(ancestorFilePath,
        x -> true,
        new ValueWithIdDeserializer<>(deserializer),
        Function.identity());
    this.ancestorStream = ancestorStream;
  }

  public AncestorExpander(Iterator<VariableValueIdPair<Long>> ancestorMappingStream,
                          Iterator<Long> ancestorStream) {
    this.ancestorMappingStream = ancestorMappingStream;
    this.ancestorStream = ancestorStream;
  }

  @Override
  public boolean hasNext() {
    setCurrentIfNotStarted();
    return this.currentAncestorMapping != null;
  }

  @Override
  public Long next() {
    setCurrentIfNotStarted();
    return nextMatch();
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      if (ancestorMappingStream.hasNext()) {
        currentAncestorMapping = ancestorMappingStream.next();
      }
      if (ancestorStream.hasNext()) {
        currentAncestor = ancestorStream.next();
      }
      hasStarted = true;
    }
  }

  private Long nextMatch() {
    if (currentAncestorMapping.getValue() != currentAncestor) {
      if (ancestorStream.hasNext()) {
        currentAncestor = ancestorStream.next();
        skipUntilMatchesAncestor(currentAncestor);
      } else {
        currentAncestor = null;
      }
    }
    Long current = currentAncestorMapping.getIndex();
    this.currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
    return current;
  }

  private void skipUntilMatchesAncestor(Long key) {
    while (currentAncestorMapping.getValue() < key) {
      this.currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
    }
  }
}
