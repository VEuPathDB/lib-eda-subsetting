package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Root of the reducer entity tree. The root of the tree represents the entity for which values are to be output.
 * When the root is reduced, it recursively delegates to its childrens' reduce methods, causing them to:
 * 1. Map streams of idIndexes output by their children to idIndexes of the ancestor entity corresponding to this node.
 * 2. Apply filters on variable values belonging to the node's entity, outputting idIndexes
 * 3. Merge idIndexes mapped from children with idIndexes output by variable filters on this node and output a single stream of ids.
 * @param <T>
 */
public class EntityJoinerRoot<T> {
  private final List<Iterator<Long>> filteredStreams;
  private final Iterator<VariableValueIdPair<T>> values;

  public EntityJoinerRoot(List<Iterator<Long>> filteredStreams, Iterator<VariableValueIdPair<T>> values) {
    this.filteredStreams = filteredStreams;
    this.values = values;
  }

  /**
   * Merge the filtered idIndex streams and map them to the values provided in the {@link EntityJoinerRoot#values} stream.
   * TODO: Merge in idIndex streams of recursively reduced child nodes.
   * @return
   */
  public Iterator<T> reduce() {
    final Comparator<Long> comparator = Comparator.naturalOrder();
    final StreamIntersectMerger<Long> intersectMerger = new StreamIntersectMerger<>(filteredStreams, comparator);
    return new ValueExtractor<>(intersectMerger, values);
  }

  private static class ValueExtractor<T> implements Iterator<T> {
    private Iterator<Long> idIndexStream;
    private Iterator<VariableValueIdPair<T>> valuesStream;
    private long currentIdIndex;
    private VariableValueIdPair<T> currentValue;

    public ValueExtractor(Iterator<Long> idIndexStream, Iterator<VariableValueIdPair<T>> valuesStream) {
      this.idIndexStream = idIndexStream;
      this.valuesStream = valuesStream;
      this.currentIdIndex = idIndexStream.hasNext() ? idIndexStream.next() : -1L;
      this.currentValue = valuesStream.hasNext() ? valuesStream.next() : null;
    }

    @Override
    public boolean hasNext() {
      return currentIdIndex != -1L && currentValue != null;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      while (currentIdIndex != currentValue.getIdIndex() && hasNext()) {
        // Advance whichever stream is trailing behind.
        if (currentIdIndex < currentValue.getIdIndex()) {
          advanceIdStream(currentValue.getIdIndex());
        } else {
          advanceValuesStream(currentIdIndex);
        }
      }
      T value = currentValue.getValue();
      currentIdIndex = idIndexStream.hasNext() ? idIndexStream.next() : -1L;
      return value;
    }

    private void advanceIdStream(long key) {
      while (currentIdIndex < key && currentIdIndex != -1L) {
        currentIdIndex = idIndexStream.hasNext() ? idIndexStream.next() : -1L;
      }
    }

    private void advanceValuesStream(long key) {
      while (currentValue.getIdIndex() < key) {
        currentValue = valuesStream.hasNext() ? valuesStream.next() : null;
      }
    }
  }
}
