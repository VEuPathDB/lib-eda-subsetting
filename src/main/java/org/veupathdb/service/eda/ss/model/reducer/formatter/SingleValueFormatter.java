package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;
import org.veupathdb.service.eda.ss.model.reducer.UnformattedTabularRecordStreamer;

import java.util.List;

public class SingleValueFormatter implements TabularValueFormatter {

  @Override
  public byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream, long idIndex) {
    return stream.next().getValue();
  }

  @Override
  public String formatString(UnformattedTabularRecordStreamer.ValueStream<String> stream, long idIndex) {
    return stream.next().getValue();
  }
}
