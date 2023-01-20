package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.util.List;

public interface TabularValueFormatter {

  byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream, long idIndex);
}
