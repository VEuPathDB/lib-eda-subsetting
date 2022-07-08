package org.veupathdb.service.eda.ss.model.reducer;

import java.util.*;
import java.util.stream.IntStream;


/**
 * This class is used to take the intersection of two or more streams of sequenced longs. The class produces a
 * stream of output longs, each of which are present in ALL input streams. This behavior is only well defined if
 * all input streams are sorted.
 */
public class StreamIntersectMerger implements Iterator<Long> {
  private final PeekableIterator[] peekableStreams;
  private final int[] nextStream;
  private Long currentElement;
  private int currentStreamIdx;
  private PeekableIterator currentStream;
  private boolean hasStarted;

  /**
   * @param sortedStreams Collection of sorted streams to merge by intersection.
   */
  public StreamIntersectMerger(List<Iterator<Long>> sortedStreams) {
    // If no input streams are provided, it should act as an "empty" Iterator.
    // If any input stream is "empty", should act as an "empty" Iterator since we are intersecting Iterators.
    if (sortedStreams.isEmpty() || !sortedStreams.stream().allMatch(Iterator::hasNext)) {
      hasStarted = true;
      peekableStreams = null;
      currentElement = null;
      nextStream = null;
      return;
    }
    // Convert iterators into peekable iterators.
    this.peekableStreams = sortedStreams.stream()
        .map(PeekableIterator::new)
        .toArray(PeekableIterator[]::new);
    // Find the stream whose first element is the largest ID index and set it to the current stream.
    // This stream's first element will be the first candidate for output. Note that we will pick an arbitrary
    // starting stream if the first elements are equal.
    this.currentStream = Arrays.stream(peekableStreams)
        .max(Comparator.comparing(iter -> iter.peek()))
        .get();
    this.currentStreamIdx = IntStream.range(0, peekableStreams.length)
        .filter(i -> peekableStreams[i] == currentStream)
        .findFirst()
        .getAsInt();
    // Cache a pointer to the nextStream to avoid excessive modulo operations.
    this.nextStream = IntStream.range(0, peekableStreams.length)
        .map(i -> i == peekableStreams.length - 1 ? 0 : i + 1)
        .toArray();
    hasStarted = false;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      findNextMatchingIdIndex();
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
      findNextMatchingIdIndex();
      hasStarted = true;
    }
    Long next = currentElement;
    currentStream.next();
    findNextMatchingIdIndex();
    return next;
  }

  /**
   * Consumes all streams until they all point an ID index that is present in all streams.
   */
  private void findNextMatchingIdIndex() {
    // This candidate should always be greater than or equal to all other
    Long candidateIdIndex = currentStream.peek();
    do {
      // Consume the stream until it matches the candidate or exceeds it. If it exceeds it, it's the new candidate
      // and we must revisit all other streams.
      candidateIdIndex = currentStream.skipUntilMatchesOrExceeds(candidateIdIndex);

      // If we reach the end of any stream, there are no matches left.
      if (candidateIdIndex == null) {
        currentElement = null;
        return;
      }
      advanceCurrentToNextStream(); // Set currentStream to next in sequence.
    } while (candidateIdIndex > currentStream.peek()); // We've overshot, continue looping
    currentElement = candidateIdIndex;
  }

  private int advanceCurrentToNextStream() {
    // This is effectively the same as (currentStreamIdx == peekableStreams.length - 1) ? 0 : currentStreamIdx + 1
    // We're caching the result of this computation now for performance, but TODO: Check if this is necessary.
    currentStreamIdx = nextStream[currentStreamIdx];
    currentStream = peekableStreams[currentStreamIdx];
    return currentStreamIdx;
  }
  
  /**
   * An iterator whose next element can be previewed without consuming it from the iterator.
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

    public Long skipUntilMatchesOrExceeds(Long key) {
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
