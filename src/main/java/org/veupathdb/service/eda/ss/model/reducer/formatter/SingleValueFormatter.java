package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.ValueStream;

public class SingleValueFormatter implements TabularValueFormatter {

  @Override
  public byte[] format(ValueStream<byte[]> stream, long idIndex) {
    return stream.next().getValue();
  }

  @Override
  public String formatString(ValueStream<String> stream, long idIndex) {
    return stream.next().getValue();
  }
}
