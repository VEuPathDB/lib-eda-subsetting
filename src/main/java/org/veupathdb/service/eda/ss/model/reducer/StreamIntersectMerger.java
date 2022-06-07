package org.veupathdb.service.eda.ss.model.reducer;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;


/**
 * This class is used to take the intersection of two or more streams of sequenced subsets.
 */
public class StreamIntersectMerger<T> implements Iterator<T> {
  private final List<PeekableIterator<T>> streams;
  private final Comparator<T> comparator;
  private T currentElement;
  private int currentStream;
  private boolean hasStarted;

  /**
   *
   * @param sortedStreams Collection of sorted streams to merge by intersection.
   * @param comparator Comparator for testing equality of elements and checking if whether we've advanced a stream
   *                   past the current element.
   */
  public StreamIntersectMerger(List<Iterator<T>> sortedStreams, Comparator<T> comparator) {
    this.streams = sortedStreams.stream()
        .map(PeekableIterator::new)
        .collect(Collectors.toList());
    this.comparator = comparator;
    this.currentStream = this.streams.indexOf(this.streams.stream()
        .filter(s -> s.hasNext())
        .max(Comparator.comparing(s -> s.next, comparator))
        .get());
    hasStarted = false;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      setCurrentElement();
      hasStarted = true;
    }
    if (currentElement == null) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public T next() {
    if (!hasStarted) {
      setCurrentElement();
      hasStarted = true;
    }
    T next = currentElement;
    streams.get(currentStream).next();
    setCurrentElement();
    return next;
  }

  /**
   * Consumes all streams until they all point an element that is equal according to the {@link Comparator}.
   */
  private void setCurrentElement() {
    T curr = streams.get(currentStream).peek();
    if (curr == null) {
      currentElement = null;
      return;
    }
    do {
      curr = streams.get(currentStream).fastForward(curr, comparator);
      if (curr == null) {
        currentElement = null;
        return;
      }
      advanceStreamPointer();
    } while (comparator.compare(curr, streams.get(currentStream).peek()) > 0); // We've overshot, continue looping
    currentElement = curr;
  }

  private int advanceStreamPointer() {
    currentStream = (currentStream + 1) % streams.size();
    return currentStream;
  }

  /**
   * TODO: Pull this into FgpUtil? If so, fastForward should be pulled up a level.
   */
  private static class PeekableIterator<T> implements Iterator<T> {
    private T next;
    private final Iterator<T> stream;

    public PeekableIterator(Iterator<T> stream) {
      this.stream = stream;
      if (stream.hasNext()) {
        this.next = stream.next();
      } else {
        this.next = null;
      }
    }

    public T fastForward(T key, Comparator<T> comparator) {
      if (next == null) {
        return null;
      }
      while (comparator.compare(next, key) < 0) {
        if (stream.hasNext()) {
          this.next = stream.next();
        } else {
          this.next = null;
          return null;
        }
      }
      return next;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    public T peek() {
      return next;
    }

    @Override
    public T next() {
      if (next == null) {
        throw new NoSuchElementException();
      } else {
        T curr = next;
        if (stream.hasNext()) {
          next = stream.next();
        } else {
          next = null;
        }
        return curr;
      }
    }
  }
}
