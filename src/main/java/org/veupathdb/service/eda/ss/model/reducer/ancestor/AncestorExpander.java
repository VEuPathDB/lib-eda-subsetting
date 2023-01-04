package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.gusdb.fgputil.iterator.CloseableIterator;
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
public class AncestorExpander implements CloseableIterator<Long> {
  private final CloseableIterator<VariableValueIdPair<Long>> descendantStream;
  private final CloseableIterator<Long> entityIdIndexStream;
  private VariableValueIdPair<Long> currentDescendant;
  private Long currentEntity;
  private boolean isInitialized = false;

  // Visible for testing
  AncestorExpander(CloseableIterator<VariableValueIdPair<Long>> descendantStream,
                   CloseableIterator<Long> entityIdIndexStream) {
    this.descendantStream = descendantStream;
    this.entityIdIndexStream = entityIdIndexStream;
  }

  @Override
  public boolean hasNext() {
    if (!isInitialized) {
      initialize();
    }
    return this.currentDescendant != null;
  }

  /**
   * @return The next descendant idIndex.
   */
  @Override
  public Long next() {
    if (!isInitialized) {
      initialize();
    }
    long toReturn = currentDescendant.getIdIndex();
    nextMatch();
    return toReturn;
  }

  /**
   * Initialization method called by hasNext() and next() to ensure that stream is not eagerly consumed by the
   * constructor. This is called on the first invocation of either of the aforementioned methods.
   */
  private void initialize() {
    currentEntity = entityIdIndexStream.hasNext() ? entityIdIndexStream.next() : null;
    nextMatch();
    if (currentEntity == null) {
      currentDescendant = null;
    }
    isInitialized = true;
  }

  /**
   * @return The next matching descendant idIndex.
   */
  private Long nextMatch() {
    // At this point, the stream should either be at the very beginning, or aligned with the most recently returned value.
    // Either way, we want to advance it one element.
    this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;

    // Check if current entity equals ancestor. If so, we can happily return it as the next element in the stream.
    // If there is a mismatch, we advance the streams until they intersect.
    if (currentDescendant != null && !Objects.equals(currentDescendant.getValue(), currentEntity)) {
      advanceStreamsUntilIntersect();
    }
    // If currentDescendant is null, stream is exhausted. Return null to indicate end of stream.
    if (currentDescendant == null) {
      return null;
    }
    return currentDescendant.getIdIndex();
  }

  private void advanceStreamsUntilIntersect() {
    while (currentDescendant != null && !Objects.equals(currentDescendant.getValue(), currentEntity) && currentEntity != null) {
      if (currentDescendant.getValue() < currentEntity) {
        // If descendants are "behind" the current entity, advance descendant stream to catch up.
        this.currentDescendant = descendantStream.hasNext() ? descendantStream.next() : null;
      } else {
        // If entities are "behind" the current descendant, advance entity stream to catch up.
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

  @Override
  public void close() throws Exception {
    descendantStream.close();
    entityIdIndexStream.close();
  }
}
