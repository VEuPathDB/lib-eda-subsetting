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
 * Takes a sorted stream of ancestor entities and a stream of tuples mapping descendants of those entities to their
 * corresponding ancestor. Produces a single column stream of a single descendant entity's identifiers corresponding
 * to the stream of ancestor entities. This effectively takes the stream of entities and "expands" it,
 * returning a stream with all IDs for at one level of descendence.
 */
public class AncestorExpander implements Iterator<Long> {
  private final Iterator<VariableValueIdPair<Long>> descendantStream;
  private final Iterator<Long> entityIdIndexStream;
  private VariableValueIdPair<Long> currentDescendant;
  private Long currentEntity;
  private boolean hasStarted = false;

  public AncestorExpander(Path descendantsFilePath,
                          AncestorDeserializer deserializer,
                          Iterator<Long> entityIdIndexStream) throws IOException {
    this.descendantStream = new FilteredValueFile(descendantsFilePath,
        x -> true,
        new ValueWithIdDeserializer<>(deserializer),
        Function.identity());
    this.entityIdIndexStream = entityIdIndexStream;
  }

  public AncestorExpander(Iterator<VariableValueIdPair<Long>> descendantStream,
                          Iterator<Long> entityIdIndexStream) {
    this.descendantStream = descendantStream;
    this.entityIdIndexStream = entityIdIndexStream;
  }

  @Override
  public boolean hasNext() {
    setCurrentIfNotStarted();
    return this.currentDescendant != null;
  }

  /**
   * @return The next descendant idIndex.
   */
  @Override
  public Long next() {
    setCurrentIfNotStarted();
    return nextMatch();
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      if (descendantStream.hasNext()) {
        currentDescendant = descendantStream.next();
      }
      if (entityIdIndexStream.hasNext()) {
        currentEntity = entityIdIndexStream.next();
      }
      hasStarted = true;
    }
  }

  /**
   * @return The next matching descendant idIndex.
   */
  private Long nextMatch() {
    // Check if current entity equals ancestor.
    if (currentDescendant.getValue() != currentEntity) {
      // Current entity is not equal to ancestor, need to skip until they match to return their descendants.
      advanceStreamsUntilIntersect();
    }
    // If entity equals ancestor, we continue iterating through descendants until the ancestor no longer matches.
    Long current = currentDescendant.getIdIndex();
    this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
    return current;
  }

  private void advanceStreamsUntilIntersect() {
    while (currentDescendant.getValue() != currentEntity && currentEntity != null) {
      if (currentDescendant.getValue() < currentEntity) {
        // If descendants are "lower" than current entity, advance descendant stream to catch up.
        this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
      } else {
        // If entities are "lower" than current descendant, advance entity stream to catch up.
        this.currentEntity = entityIdIndexStream.hasNext() ? entityIdIndexStream.next() : null;
      }
    }
  }
}
