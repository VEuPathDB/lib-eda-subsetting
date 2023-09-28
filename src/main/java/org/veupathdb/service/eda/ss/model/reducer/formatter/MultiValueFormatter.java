package org.veupathdb.service.eda.ss.model.reducer.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.gusdb.fgputil.json.JsonUtil;
import org.veupathdb.service.eda.ss.model.reducer.FormattedTabularRecordStreamer;
import org.veupathdb.service.eda.ss.model.reducer.UnformattedTabularRecordStreamer;

import java.util.ArrayList;
import java.util.List;

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

  @Override
  public String formatString(UnformattedTabularRecordStreamer.ValueStream<String> stream, long idIndex) {
    List<String> allRecords = new ArrayList<>();
    while (stream.hasNext() && stream.peek().getIdIndex() == idIndex) {
      allRecords.add(stream.next().getValue());
    }
    try {
      return JsonUtil.Jackson.writeValueAsString(allRecords);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
