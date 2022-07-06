package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.variable.VariableValueIdPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ValueExtractor implements Iterator<List<String>> {
  private List<ValueStream> streams;
  private Iterator<Long> idIndexStream;
  private Long currentIndex;

  public ValueExtractor(List<Iterator<VariableValueIdPair<?>>> streams, Iterator<Long> idIndexStream) {
    this.streams = streams.stream()
        .map(s -> new ValueStream(s))
        .collect(Collectors.toList());
    this.idIndexStream = idIndexStream;
    if (idIndexStream.hasNext()) {
      currentIndex = idIndexStream.next();
    }
  }

  @Override
  public boolean hasNext() {
    return currentIndex != null;
  }

  @Override
  public List<String> next() {
    List<String> record = new ArrayList<>();
    for (ValueStream stream: streams) {
      if (!stream.hasNext()) {
        record.add("");
        continue;
      }
      while (stream.peek().getIdIndex() < currentIndex) {
        stream.next();
      }
      if (stream.peek().getIdIndex() == currentIndex) {
        record.add(stream.next().getValue().toString());
      } else {
        record.add("");
      }
    }
    if (idIndexStream.hasNext()) {
      currentIndex = idIndexStream.next();
    } else {
      currentIndex = null;
    }
    return record;
  }

  private static class ValueStream implements Iterator<VariableValueIdPair> {
    private VariableValueIdPair next;
    private Iterator<VariableValueIdPair<?>> stream;

    public ValueStream(Iterator<VariableValueIdPair<?>> stream) {
      if (stream.hasNext()) {
        next = stream.next();
      }
      this.stream = stream;
    }

    public VariableValueIdPair peek() {
      return next;
    }

    @Override
    public boolean hasNext() {
      return next != null;
    }

    @Override
    public VariableValueIdPair next() {
      VariableValueIdPair curr = next;
      if (stream.hasNext()) {
        next = stream.next();
      } else {
        next = null;
      }
      return curr;
    }
  }
}
