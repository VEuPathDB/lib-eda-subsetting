package org.veupathdb.service.eda.ss.model.reducer;

import java.util.*;

/**
 * Merges two or more sorted streams of ID indexes, outputting a sorted stream of sorted ID indexes which represents
 * a union of the input streams.
 *
 * This is done by maintaining a min heap containing the next element from each input iterator. After removing an
 * element from the heap, we add the next element from that stream.
 */
public class StreamUnionMerger implements Iterator<Long> {
  // A possible optimization is to use a slimmed-down min-heap as opposed to a PriorityQueue (implemented as a heap).
  private PriorityQueue<HeapElement> minHeap;
  private long lastIdIndex;

  public StreamUnionMerger(List<Iterator<Long>> streams) {
    this.minHeap = new PriorityQueue<>(streams.size(), Comparator.comparing(element -> element.idIndex));
    // Add the first element from each stream to the min heap
    streams.forEach(stream -> {
      if (stream.hasNext()) {
        minHeap.add(new HeapElement(stream, stream.next()));
      }
    });
  }

  @Override
  public boolean hasNext() {
    return !minHeap.isEmpty();
  }

  @Override
  public Long next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No elements left to union in input streams.");
    }
    HeapElement next;
    do {
      // Grab the smallest element seen thus far (i.e. the root of the min heap)
      next = minHeap.poll();
      if (next.stream.hasNext()) {
        // Re-hydrate the min-heap with the next element from the stream that contained the previous min.
        minHeap.add(new HeapElement(next.stream, next.stream.next()));
      }
      // Skip this element if it has already been returned for the sake of omitting duplicates.
    } while (next.idIndex == lastIdIndex);

    // Set the lastIdIndex variable to allow us to omit duplicates on subsequent invocations of next().
    lastIdIndex = next.idIndex;
    return next.idIndex;
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
