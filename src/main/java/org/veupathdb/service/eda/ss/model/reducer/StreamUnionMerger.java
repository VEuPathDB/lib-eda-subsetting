package org.veupathdb.service.eda.ss.model.reducer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.iterator.CloseableIterator;
import org.veupathdb.service.eda.ss.model.Entity;

import java.util.*;

/**
 * Merges two or more sorted streams of ID indexes, outputting a sorted stream of sorted ID indexes which represents
 * a union of the input streams.
 *
 * This is done by maintaining a min heap containing the next element from each input iterator. After removing an
 * element from the heap, we add the next element from that stream.
 */
public class StreamUnionMerger implements CloseableIterator<Long> {
  private static final Logger LOG = LogManager.getLogger(StreamUnionMerger.class);

  // A possible optimization is to use a slimmed-down min-heap as opposed to a PriorityQueue (implemented as a heap).
  private PriorityQueue<HeapElement> minHeap;
  private List<CloseableIterator<Long>> streams;
  private long lastIdIndex;
  private boolean first;
  private Long next;

  public StreamUnionMerger(List<CloseableIterator<Long>> streams) {
    this.minHeap = new PriorityQueue<>(streams.size(), Comparator.comparing(element -> element.idIndex));
    this.streams = streams;
    // Add the first element from each stream to the min heap
    streams.forEach(stream -> {
      if (stream.hasNext()) {
        minHeap.add(new HeapElement(stream, stream.next()));
      }
    });
    first = true;
    setNext();
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Long next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No elements left to union in input streams.");
    }
    Long curr = next;
    setNext();
    return curr;
  }

  private void setNext() {
    HeapElement nextElement;
    if (minHeap.isEmpty()) {
      next = null;
      return;
    }
    do {
      // Grab the smallest element seen thus far (i.e. the root of the min heap)
      nextElement = minHeap.poll();
      if (nextElement.stream.hasNext()) {
        // Re-hydrate the min-heap with the next element from the stream that contained the previous min.
        minHeap.add(new HeapElement(nextElement.stream, nextElement.stream.next()));
      } else {
        if (minHeap.isEmpty()) {
          if (nextElement.idIndex == lastIdIndex) {
            this.next = null;
          } else {
            this.next = nextElement.idIndex;
          }
          return;
        }
      }
      // Skip this element if it has already been returned for the sake of omitting duplicates.
    } while (nextElement.idIndex == lastIdIndex && !first);

    first = false;
    // Set the lastIdIndex variable to allow us to omit duplicates on subsequent invocations of next().
    lastIdIndex = nextElement.idIndex;
    next = nextElement.idIndex;
  }

  @Override
  public void close() throws Exception {
    streams.forEach(stream -> {
      try {
        stream.close();
      } catch (Exception e) {
        LOG.warn("Failed to close stream.", e);
      }
    });
  }

  private static class HeapElement {
    private Iterator<Long> stream;
    private Long idIndex;

    public HeapElement(Iterator<Long> stream, Long idIndex) {
      this.stream = stream;
      this.idIndex = idIndex;
    }
  }
}
