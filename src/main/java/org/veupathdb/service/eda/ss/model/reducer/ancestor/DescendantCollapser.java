package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.veupathdb.service.eda.ss.model.reducer.FilteredValueFile;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.converter.AncestorFileDeserializer;
import org.veupathdb.service.eda.ss.model.variable.converter.ValueWithIdDeserializer;

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
  private final Iterator<Long> descendantStream;
  private VariableValueIdPair<Long> currentAncestorMapping;
  private Long currentDescendant;
  private Long lastAncestor;
  private boolean hasStarted = false;

  public DescendantCollapser(Path ancestorFilePath,
                             AncestorFileDeserializer deserializer,
                             Iterator<Long> descendantStream) throws IOException {
    this.ancestorMappingStream = new FilteredValueFile(ancestorFilePath,
        x -> true,
        new ValueWithIdDeserializer<>(deserializer),
        Function.identity());
    this.descendantStream = descendantStream;
  }

  public DescendantCollapser(Iterator<VariableValueIdPair<Long>> ancestorMappingStream,
                             Iterator<Long> descendantStream) {
    this.ancestorMappingStream = ancestorMappingStream;
    this.descendantStream = descendantStream;
  }

  @Override
  public boolean hasNext() {
    setCurrentIfNotStarted();
    return this.currentAncestorMapping != null && this.currentDescendant != null;
  }

  @Override
  public Long next() {
    setCurrentIfNotStarted();
    return nextMatch();
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
      currentDescendant = descendantStream.hasNext() ?  descendantStream.next() : null;
      hasStarted = true;
    }
  }

  private Long nextMatch() {
    // Continue until currentDescendant matches ancestor mapping's descendant, so we can return the parent.
    // If the parent is the same as lastAncestor, we also want to continue since we've already returned the parent.
    while (currentAncestorMapping.getIndex() != currentDescendant || lastAncestor == currentAncestorMapping.getValue()) {
      if (currentAncestorMapping.getIndex() > currentDescendant) {
        // Advance the input entity stream.
        this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
      }
      this.currentAncestorMapping = ancestorMappingStream.hasNext() ? ancestorMappingStream.next() : null;
      if (this.currentDescendant == null || this.currentAncestorMapping == null) {
        break;
      }
    }
    lastAncestor = currentAncestorMapping.getValue();
    currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
    return lastAncestor;
  }
}
