package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;
import org.veupathdb.service.eda.ss.model.reducer.UnformattedTabularRecordStreamer;

import java.util.List;

public interface TabularValueFormatter {

  byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream, long idIndex);

  String formatString(UnformattedTabularRecordStreamer.ValueStream<String> stream, long idIndex);
}
