package org.veupathdb.service.eda.ss.model.reducer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * This class is used to take the intersection of two or more streams of sequenced subsets.
 */
public class StreamIntersectMerger implements Iterator<Long> {
  private final PeekableIterator[] streams;
//  private final Comparator<Long> comparator;
  private Long currentElement;
  private int currentStreamIdx;
  private PeekableIterator currentStream;
  private boolean hasStarted;

  /**
   *
   * @param sortedStreams Collection of sorted streams to merge by intersection.
   */
  public StreamIntersectMerger(List<Iterator<Long>> sortedStreams) {
    this.streams = sortedStreams.stream()
        .map(PeekableIterator::new)
        .toArray(PeekableIterator[]::new);
    this.currentStream = Arrays.stream(streams)
        .max(Comparator.comparing(iter -> iter.peek()))
        .get();
    this.currentStreamIdx = IntStream.range(0, streams.length)
        .filter(i -> streams[i] == currentStream)
        .findFirst()
        .getAsInt();
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
  public Long next() {
    if (!hasStarted) {
      setCurrentElement();
      hasStarted = true;
    }
    Long next = currentElement;
    currentStream.next();
    setCurrentElement();
    return next;
  }

  /**
   * Consumes all streams until they all point an element that is equal according to the {@link Comparator}.
   */
  private void setCurrentElement() {
    Long curr = currentStream.peek();
    do {
      curr = currentStream.fastForward(curr);
      if (curr == null) {
        currentElement = null;
        return;
      }
      advanceStreamPointer();
    } while (curr > currentStream.peek()); // We've overshot, continue looping
    currentElement = curr;
  }

  private int advanceStreamPointer() {
    currentStreamIdx = (currentStreamIdx + 1) % streams.length;
    currentStream = streams[currentStreamIdx];
    return currentStreamIdx;
  }

  /**
   * TODO: Pull this into FgpUtil? If so, fastForward should be pulled up a level.
   */
  private static class PeekableIterator implements Iterator<Long> {
    private Long next;
    private final Iterator<Long> stream;

    public PeekableIterator(Iterator<Long> stream) {
      this.stream = stream;
      if (stream.hasNext()) {
        this.next = stream.next();
      } else {
        this.next = null;
      }
    }

    public Long fastForward(Long key) {
      if (next == null) {
        return null;
      }
      while (next < key) {
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

    public Long peek() {
      return next;
    }

    @Override
    public Long next() {
      if (next == null) {
        throw new NoSuchElementException();
      } else {
        Long curr = next;
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
