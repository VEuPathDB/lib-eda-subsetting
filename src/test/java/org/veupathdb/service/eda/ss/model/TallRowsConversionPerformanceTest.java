package org.veupathdb.service.eda.ss.model;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import org.gusdb.fgputil.Timer;
import org.junit.jupiter.api.Test;

public class TallRowsConversionPerformanceTest {

  private static final int NUM_RECORDS_TO_PROCESS = 200000;

  @Test
  public void doTallRowsPerfTest() throws IOException {
    Entity entity = new TestModel().participant;
    List<String> outputColumns = StudySubsettingUtils.getTabularOutputColumns(entity, entity.getVariables());
    TallRowsGeneratedResultIterator iterator = new TallRowsGeneratedResultIterator(entity, NUM_RECORDS_TO_PROCESS);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/rdoherty/tabular_results.txt"))) {
      Timer t = new Timer();
      StudySubsettingUtils.writeWideRows(iterator, writer, outputColumns, entity);
      writer.flush();
      System.out.println("Time to dump " + NUM_RECORDS_TO_PROCESS + " entity records: " + t.getElapsedString());
    }
  }

}
