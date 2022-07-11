package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.reducer.FilteredValueIterator;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.AncestorDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * Given a sorted stream of entities and a mapping of those entities to their ancestors, returns a stream of ancestors
 * where each ancestor entity return has at least one descendant in the stream of descendant entities.
 */
public class DescendantCollapser implements Iterator<Long> {
  private final Iterator<VariableValueIdPair<Long>> ancestorStream;
  private final Iterator<Long> currentEntityStream;
  private VariableValueIdPair<Long> currentAncestor;
  private Long currentEntity;
  private Long lastAncestor;
  private boolean hasStarted = false;

  public DescendantCollapser(Path ancestorFilePath,
                             AncestorDeserializer deserializer,
                             Iterator<Long> currentEntityStream) throws IOException {
    this.ancestorStream = new FilteredValueIterator<>(ancestorFilePath,
        x -> true,
        deserializer,
        Function.identity());
    this.currentEntityStream = currentEntityStream;
  }

  public DescendantCollapser(Iterator<VariableValueIdPair<Long>> ancestorStream,
                             Iterator<Long> currentEntityStream) {
    this.ancestorStream = ancestorStream;
    this.currentEntityStream = currentEntityStream;
  }

  @Override
  public boolean hasNext() {
    setCurrentIfNotStarted();
    return this.currentAncestor != null && this.currentEntity != null;
  }

  @Override
  public Long next() {
    setCurrentIfNotStarted();
    return nextMatch();
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      currentAncestor = ancestorStream.hasNext() ? ancestorStream.next() : null;
      currentEntity = currentEntityStream.hasNext() ?  currentEntityStream.next() : null;
      hasStarted = true;
    }
  }

  private Long nextMatch() {
    // Continue until currentDescendant matches ancestor mapping's descendant, so we can return the parent.
    // If the parent is the same as lastAncestor, we also want to continue since we've already returned the parent.
    while (currentAncestor.getIdIndex() != currentEntity || Objects.equals(lastAncestor, currentAncestor.getValue())) {
      if (currentAncestor.getIdIndex() > currentEntity) {
        // Advance the input entity stream if ancestor is greater to catch up.
        this.currentEntity = currentEntityStream.hasNext() ? currentEntityStream.next() : null;
      } else {
        // If ancestor is less than or equal to currentEntity, we advance it.
        this.currentAncestor = ancestorStream.hasNext() ? ancestorStream.next() : null;
      }
      if (this.currentEntity == null || this.currentAncestor == null) {
        return null;
      }
    }
    lastAncestor = currentAncestor.getValue();
    currentEntity = currentEntityStream.hasNext() ? currentEntityStream.next() : null;
    return lastAncestor;
  }
}
