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
 * Takes a sorted stream of entities and a stream of tuples mapping descendants of those entities to their corresponding
 * ancestor. Produces a stream of the descendant entity identifiers corresponding to the stream of entities. This
 * effectively takes the stream of entities and "expands" it, returning a stream with all descendants.
 */
public class AncestorExpander implements Iterator<Long> {
  private final Iterator<VariableValueIdPair<Long>> ancestorMappingStream;
  private final Iterator<Long> ancestorStream;
  private VariableValueIdPair<Long> currentAncestorMapping;
  private Long currentAncestor;
  private boolean hasStarted = false;

  public AncestorExpander(Path ancestorFilePath,
                          AncestorDeserializer deserializer,
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
    // Check if current entity equals ancestor. If so, we continue iterating through descendants until they don't match.
    if (currentAncestorMapping.getValue() != currentAncestor) {
      // Current entity is not equal to ancestor, need to skip until they match to return their descendants.
      if (ancestorStream.hasNext()) {
        currentAncestor = ancestorStream.next();
        skipUntilMatchesAncestor(currentAncestor);
      } else {
        // Used to indicate the stream is exhausted.
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
