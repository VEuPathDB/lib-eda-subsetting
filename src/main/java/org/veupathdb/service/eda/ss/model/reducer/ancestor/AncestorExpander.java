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
    this(new FilteredValueIterator(descendantsFilePath,
        x -> true,
        deserializer,
        Function.identity()), entityIdIndexStream);
  }

  // Visible for testing
  AncestorExpander(Iterator<VariableValueIdPair<Long>> descendantStream,
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
    long toReturn = currentDescendant.getIdIndex();
    nextMatch();
    return toReturn;
  }

  private void setCurrentIfNotStarted() {
    if (!hasStarted) {
      nextMatch();
      hasStarted = true;
    }
  }

  /**
   * @return The next matching descendant idIndex.
   */
  private Long nextMatch() {
    if (!hasStarted) {
      currentEntity = entityIdIndexStream.hasNext() ? entityIdIndexStream.next() : null;
    }
    this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
    // Check if current entity equals ancestor.
    // If entity equals ancestor, we continue iterating through descendants until the ancestor no longer matches.
    if (currentDescendant != null && !Objects.equals(currentDescendant.getValue(), currentEntity)) {
      // Current entity is not equal to ancestor, need to skip until they match to return their descendants.
      advanceStreamsUntilIntersect();
    }
    // If currentDescendant is null, stream is exhausted.
    if (currentDescendant == null) {
      return null;
    }
    return currentDescendant.getIdIndex();
  }

  private void advanceStreamsUntilIntersect() {
    while (currentDescendant != null && !Objects.equals(currentDescendant.getValue(), currentEntity) && currentEntity != null) {
      if (currentDescendant.getValue() < currentEntity) {
        // If descendants are "lower" than current entity, advance descendant stream to catch up.
        this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
      } else {
        // If entities are "lower" than current descendant, advance entity stream to catch up.
        if (entityIdIndexStream.hasNext()) {
          this.currentEntity = entityIdIndexStream.next();
        } else {
          // If we hit end of entity stream, signify that the stream is exhausted.
          this.currentEntity = null;
          this.currentDescendant = null;
        }
      }
    }
  }
}
