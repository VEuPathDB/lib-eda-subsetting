package org.veupathdb.service.eda.ss.model.reducer;

import java.util.*;


/**
 * This class is used to take the intersection of two or more streams of sequenced longs. The class produces a
 * stream of output longs, each of which are present in ALL input streams. This behavior is only well defined if
 * all input streams are sorted.
 */
public class StreamIntersectMerger implements Iterator<Long> {
  private final PeekableIterator[] peekableStreams;
  private Long nextOutputIndex;
  private PeekableIterator currentStream;
  private boolean hasStarted;
  private RingLinkedList streamRing;

  /**
   * @param sortedStreams Collection of sorted streams to merge by intersection.
   */
  public StreamIntersectMerger(List<Iterator<Long>> sortedStreams) {
    // If no input streams are provided, it should act as an "empty" Iterator.
    // If any input stream is "empty", should act as an "empty" Iterator since we are intersecting Iterators.
    if (sortedStreams.isEmpty() || !sortedStreams.stream().allMatch(Iterator::hasNext)) {
      hasStarted = true;
      peekableStreams = null;
      nextOutputIndex = null;
      return;
    }
    // Convert iterators into peekable iterators.
    this.peekableStreams = sortedStreams.stream()
        .map(PeekableIterator::new)
        .toArray(PeekableIterator[]::new);
    this.streamRing = new RingLinkedList(peekableStreams);
    this.currentStream = streamRing.cursor.iterator;
    hasStarted = false;
  }

  @Override
  public boolean hasNext() {
    if (!hasStarted) {
      findNextMatchingIdIndex();
      hasStarted = true;
    }
    return nextOutputIndex != null;
  }

  @Override
  public Long next() {
    if (!hasStarted) {
      findNextMatchingIdIndex();
      hasStarted = true;
    }
    Long next = nextOutputIndex;
    currentStream.next();
    findNextMatchingIdIndex();
    return next;
  }

  private void findNextMatchingIdIndex() {
    boolean matchFound = false;
    while (!matchFound && currentStream.hasNext()) {
      Long candidateIdIndex = currentStream.peek();
      // Iterate through each stream in the stream ring.
      for (int i = 0; i < peekableStreams.length; i++) {
        currentStream = streamRing.advanceCursor().iterator;
        // Advance the stream until it matches the candidate. If we overshoot, break and start our loop over
        // with the ID index we found as our new candidate.
        Long idIndex = currentStream.skipUntilMatchesOrExceeds(candidateIdIndex);
        if (!Objects.equals(idIndex, candidateIdIndex)) {
          break;
        }
        // If we've iterated through all the streams without breaking, indicate a match was found.
        if (i == peekableStreams.length - 1) {
          nextOutputIndex = candidateIdIndex;
          matchFound = true;
        }
      }
    }
    if (!currentStream.hasNext()) {
      // Indicate end of iterator if we hit the end of any stream.
      nextOutputIndex = null;
    }
  }

  /**
   * Utility class which stores a linked list of PeekableIterators. The elements are linked in a ring-like structure
   * such that the last element points to the first to allow for natural traversal of streams.
   */
  private static class RingLinkedList {
    private Node cursor;

    public RingLinkedList(PeekableIterator[] iterators) {
      Node first = new Node(iterators[0]);
      cursor = first;
      for (int i = 0; i < iterators.length; i++) {
        if (i == iterators.length - 1) {
          cursor.next = first;
        } else {
          cursor.next = new Node(iterators[i + 1]);
        }
        cursor = cursor.next;
      }
    }

    public Node advanceCursor() {
      this.cursor = cursor.next;
      return this.cursor;
    }

    private static class Node {
      private final PeekableIterator iterator;
      private Node next;

      public Node(PeekableIterator peekableIterator) {
        this.iterator = peekableIterator;
      }
    }
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

    /**
     * Advance the stream until the next element either matches or exceeds the key.
     * @param key Value to advance the stream to or past if the key is not present in the stream.
     * @return Value the stream is advanced to, either key or the next highest value of the stream.
     */
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
