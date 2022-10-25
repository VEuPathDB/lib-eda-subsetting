package org.veupathdb.service.eda.ss.model.reducer.ancestor;

import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.reducer.FilteredValueIterator;
import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;
import org.veupathdb.service.eda.ss.model.variable.binary.AncestorDeserializer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

/**
 * Given a sorted stream of entities and a mapping of those entities to their ancestors, returns a stream of ancestors
 * where each ancestor entity return has at least one descendant in the stream of descendant entities.
 */
public class DescendantCollapser implements CloseableIterator<Long> {
  private final CloseableIterator<VariableValueIdPair<Long>> ancestorStream;
  private final CloseableIterator<Long> currentEntityStream;
  private VariableValueIdPair<Long> currentAncestor;
  private Long currentEntity;
  private boolean initialized = false;
  private Long matchedAncestor;

  public DescendantCollapser(Path ancestorFilePath,
                             AncestorDeserializer deserializer,
                             CloseableIterator<Long> currentEntityStream) throws IOException {
    this.ancestorStream = new FilteredValueIterator<>(ancestorFilePath,
        x -> true,
        deserializer,
        Function.identity());
    this.currentEntityStream = currentEntityStream;
  }

  public DescendantCollapser(CloseableIterator<VariableValueIdPair<Long>> ancestorStream,
                             CloseableIterator<Long> currentEntityStream) {
    this.ancestorStream = ancestorStream;
    this.currentEntityStream = currentEntityStream;
  }

  @Override
  public boolean hasNext() {
    if (!initialized) {
      initialize();
    }
    return currentEntity != null && currentAncestor != null;
  }

  @Override
  public Long next() {
    if (!initialized) {
      initialize();
    }
    Long toReturn = matchedAncestor;
    nextMatch(false);
    return toReturn;
  }

  private void initialize() {
    currentEntity = currentEntityStream.hasNext() ? currentEntityStream.next() : null;
    currentAncestor = ancestorStream.hasNext() ? ancestorStream.next() : null;
    if (currentEntity == null || currentAncestor == null) {
      return;
    }
    // Indicate that this is the first match attempt
    nextMatch(true);
    initialized = true;
  }

  private void nextMatch(boolean firstMatch) {
    if (!firstMatch) {
      // If this is not our first match, advance the currentEntityStream by one element since the previous was already returned
      if (currentEntityStream.hasNext()) {
        currentEntity = currentEntityStream.next();
      } else {
        currentEntity = null;
        return;
      }
    }
    // Continue looping until we find an entity that matches the ancestor stream.
    while (!currentEntity.equals(currentAncestor.getIdIndex()) || Objects.equals(matchedAncestor, currentAncestor.getValue())) {
      // Check which is ahead, ancestor stream or entity stream and advance one accordingly.
      if (currentEntity > currentAncestor.getIdIndex()) {
        if (ancestorStream.hasNext()) {
          currentAncestor = ancestorStream.next();
        } else {
          // Either stream ending indicates the end of output stream. No more matches will be found.
          currentAncestor = null;
          return;
        }
      } else {
        if (currentEntityStream.hasNext()) {
          currentEntity = currentEntityStream.next();
        } else {
          // Either stream ending indicates the end of output stream. No more matches will be found.
          currentEntity = null;
          return;
        }
      }
    }
    // Keep track of matched ancestor, as we only want to return the ancestor once even if multiple entities have the same
    // ancestor in the entity stream.
    matchedAncestor = currentAncestor.getValue();
  }

  @Override
  public void close() throws Exception {
    ancestorStream.close();
    currentEntityStream.close();
  }
}
