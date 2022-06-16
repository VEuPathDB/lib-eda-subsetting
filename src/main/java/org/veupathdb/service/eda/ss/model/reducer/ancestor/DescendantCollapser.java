package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.reducer.FilteredValueFile;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.AncestorDeserializer;
import org.veupathdb.service.eda.ss.model.variable.binary.ValueWithIdDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Given a sorted stream of entities and a mapping of those entities to their ancestors, returns a stream of ancestors
 * where each ancestor entity return has at least one descendant in the stream of descendant entities.
 */
public class DescendantCollapser implements Iterator<Long> {
  private final Iterator<VariableValueIdPair<Long>> ancestorMappingStream;
  private final Iterator<Long> currentEntityStream;
  private VariableValueIdPair<Long> currentAncestorMapping;
  private Long currentEntity;
  private Long lastAncestor;
  private boolean hasStarted = false;

  public DescendantCollapser(Path ancestorFilePath,
                             AncestorDeserializer deserializer,
                             Iterator<Long> currentEntityStream) throws IOException {
    this.ancestorMappingStream = new FilteredValueFile(ancestorFilePath,
        x -> true,
        new ValueWithIdDeserializer<>(deserializer),
        Function.identity());
    this.currentEntityStream = currentEntityStream;
  }

  public DescendantCollapser(Iterator<VariableValueIdPair<Long>> ancestorMappingStream,
                             Iterator<Long> currentEntityStream) {
    this.ancestorMappingStream = ancestorMappingStream;
    this.currentEntityStream = currentEntityStream;
  }

  @Override
  public boolean hasNext() {
    setCurrentIfNotStarted();
    return this.currentAncestorMapping != null && this.currentEntity != null;
  }

  @Override
  public Long next() {
    setCurrentIfNotStarted();
    return nextMatch();
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
      currentEntity = currentEntityStream.hasNext() ?  currentEntityStream.next() : null;
      hasStarted = true;
    }
  }

  private Long nextMatch() {
    // Continue until currentDescendant matches ancestor mapping's descendant, so we can return the parent.
    // If the parent is the same as lastAncestor, we also want to continue since we've already returned the parent.
    while (currentAncestorMapping.getIdIndex() != currentEntity || lastAncestor == currentAncestorMapping.getValue()) {
      if (currentAncestorMapping.getIdIndex() > currentEntity) {
        // Advance the input entity stream.
        this.currentEntity = currentEntityStream.hasNext() ? currentEntityStream.next() : null;
      }
      this.currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
      if (this.currentEntity == null || this.currentAncestorMapping == null) {
        break;
      }
    }
    lastAncestor = currentAncestorMapping.getValue();
    currentEntity = currentEntityStream.hasNext() ? currentEntityStream.next() : null;
    return lastAncestor;
  }
}
