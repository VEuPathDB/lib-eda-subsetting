package org.veupathdb.service.eda.ss.model.reducer.formatter;

import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MultiValueFormatter implements TabularValueFormatter {

  @Override
  public byte[] format(FormattedTabularRecordStreamer.ValueStream<byte[]> stream,
                       long idIndex) {
    List<byte[]> allRecords = new ArrayList<>();
    while (stream.hasNext() && stream.peek().getIdIndex() == idIndex) {
      allRecords.add(stream.next().getValue());
    }
    final int numCommas = allRecords.size() - 1;
    final byte[] result = new byte[numCommas + allRecords.stream().mapToInt(arr -> arr.length).sum() + 2];
    result[0] = '[';
    result[result.length - 1] = ']';
    int index = 1;
    int recordCount = 0;
    for (byte[] record: allRecords) {
      System.arraycopy(record, 0, result, index, record.length);
      index += record.length;
      if (recordCount != allRecords.size() - 1) {
        result[index++] = ',';
      }
      recordCount++;
    }
    return result;
  }
}
