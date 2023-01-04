package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.iterator.CloseableIterator;

import java.util.*;


/**
 * This class is used to take the intersection of two or more streams of sequenced longs. The class produces a
 * stream of output longs, each of which are present in ALL input streams. This behavior is only well defined if
 * all input streams are sorted.
 */
public class StreamIntersectMerger implements CloseableIterator<Long> {
  private static final Logger LOG = LogManager.getLogger(StreamIntersectMerger.class);

  private final PeekableIterator[] peekableIdIndexStreams;
  private Long nextOutputIndex;
  private PeekableIterator currentIdIndexStream;
  private RingLinkedList streamRing;
  private boolean initialized;

  /**
   * @param sortedStreams Collection of sorted streams to merge by intersection.
   */
  public StreamIntersectMerger(List<CloseableIterator<Long>> sortedStreams) {
    // If no input streams are provided, it should act as an "empty" Iterator.
    // If any input stream is "empty", should act as an "empty" Iterator since we are intersecting Iterators.
    if (sortedStreams.isEmpty() || !sortedStreams.stream().allMatch(Iterator::hasNext)) {
      initialized = true;
      peekableIdIndexStreams = null;
      nextOutputIndex = null;
      return;
    }
    if (sortedStreams.size() < 2) {
      throw new IllegalArgumentException("Stream intersect merger must operate on two or more data streams");
    }
    // Convert iterators into peekable iterators.
    this.peekableIdIndexStreams = sortedStreams.stream()
        .map(PeekableIterator::new)
        .toArray(PeekableIterator[]::new);
    this.streamRing = new RingLinkedList(peekableIdIndexStreams);
    this.currentIdIndexStream = streamRing.cursor.currentStream;
    this.initialized = false;
  }

  /**
   * Initialization method called by hasNext() and next() to ensure that stream is not eagerly consumed by the
   * constructor. This is called on the first invocation of either of the aforementioned methods.
   */
  public void initialize() {
    findNextMatchingIdIndex();
    initialized = true;
  }

  @Override
  public boolean hasNext() {
    if (!initialized) {
      initialize();
    }
    return nextOutputIndex != null;
  }

  @Override
  public Long next() {
    if (!initialized) {
      initialize();
    }
    Long next = nextOutputIndex;
    if (next == null) {
      throw new NoSuchElementException("No more elements in the iterator");
    }
    findNextMatchingIdIndex();
    return next;
  }

  private void findNextMatchingIdIndex() {
    // set candidate to current stream's value
    Long candidateIdIndex = currentIdIndexStream.peek();
    if (candidateIdIndex == null) {
      nextOutputIndex = null;
      return;
    }
    // value not null; counts as first concurring stream
     int numConcurringStreams = 1;

    // continual loop, trying to match all iterators to the same value
    while (numConcurringStreams < streamRing.size()) {

      // move to the next stream
      currentIdIndexStream = streamRing.advanceCursor().currentStream;

      // find a value >= our candidate
      Long idIndex = currentIdIndexStream.skipUntilMatchesOrExceeds(candidateIdIndex);

      // operations based on retrieved value
      if (idIndex == null) {
        // reached the end of the stream; no more matches ever
        nextOutputIndex = null;
        return;
      }
      else if (idIndex > candidateIdIndex) {
        // current candidate is no good; use the higher value and reset count
        candidateIdIndex = idIndex;
        numConcurringStreams = 1;
      }
      else { // idIndex == candidateIdIndex
        // current stream matches candidate; increment count and continue with this value
        numConcurringStreams++;
      }
    }

    // able to exit loop without returning null first; must have found a match
    nextOutputIndex = candidateIdIndex;
  }

  @Override
  public void close() throws Exception {
    if (peekableIdIndexStreams != null) {
      Arrays.stream(peekableIdIndexStreams)
          .forEach(stream -> {
            try {
              stream.close();
            } catch (Exception e) {
              LOG.warn("Unable to close stream.", e);
            }
          });
    }
  }

  /**
  /**
   * Utility class which stores a linked list of PeekableIterators. The elements are linked in a ring-like structure
   * such that the last element points to the first to allow for natural traversal of streams.
   */
  private static class RingLinkedList {
    private Node cursor;
    private int size;

    public RingLinkedList(PeekableIterator[] iterators) {
      this.size = iterators.length;
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

    public int size() {
      return size;
    }

    public Node advanceCursor() {
      this.cursor = cursor.next;
      return this.cursor;
    }

    private static class Node {
      private final PeekableIterator currentStream;
      private Node next;

      public Node(PeekableIterator peekableIterator) {
        this.currentStream = peekableIterator;
      }
    }
  }

  /**
   * An iterator whose next element can be previewed without consuming it from the iterator.
   */
  protected static class PeekableIterator implements CloseableIterator<Long> {
    private Long next;
    private final CloseableIterator<Long> stream;

    public PeekableIterator(CloseableIterator<Long> stream) {
      this.stream = stream;
      if (stream.hasNext()) {
        this.next = stream.next();
      } else {
        this.next = null;
      }
    }

    /**
     * Advance the stream until the next element either matches or exceeds the ID index key.
     * If the same ID index appears multiple times in the stream, advances the stream to the last duplicate.
     * Duplicate IDs can be present for multi-value variables, once for each value. When duplicate entities appear in the stream,
     * we only want to return the entity ID index once to ensure that the resulting stream doesn't have duplicates.
     *
     * @param key Value to advance the stream to or past if the key is not present in the stream.
     * @return Value the stream is advanced to, either key or the next highest value of the stream.
     */
    public Long skipUntilMatchesOrExceeds(Long key) {
      if (next == null) {
        return null;
      }
      Long prev = null;
      // Skip until our cursor moves past our target, so we can be sure prev points to the last instance of our
      // target ID index.
      while (next <= key) {
        prev = next;
        if (stream.hasNext()) {
          this.next = stream.next();
        } else {
          this.next = null;
          return prev.equals(key) ? prev : null;
        }
      }
      if (prev == null || prev < key) {
        // prev < key implies there are no duplicates of prev. This means it has already been returned, so we return next.
        return next;
      } else {
        // prev == key implies that we had to skip some duplicates in the loop above. Return prev, to ensure we do not
        // skip all values with ID index equal to key.
        return prev;
      }
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

    @Override
    public void close() throws Exception {
      stream.close();
    }
  }
}
