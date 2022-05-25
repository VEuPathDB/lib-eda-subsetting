package org.veupathdb.service.eda.ss.model.db;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.Timer;
import org.gusdb.fgputil.functional.FunctionalInterfaces.SupplierWithException;
import org.junit.jupiter.api.Test;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.test.MockModel;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;

public class TallRowsConversionPerformanceTest {

  private static final Logger LOG = LogManager.getLogger(TallRowsConversionPerformanceTest.class);

  // specify the entity to convert
  private static final Function<MockModel, Entity> TEST_ENTITY = m -> m.observation;

  // specify how many records of that entity to convert
  private static final int NUM_RECORDS_TO_PROCESS = 20000;

  // whether to cache a sample record (can help determine perf bottleneck)
  private static final boolean CACHE_SAMPLE_RECORD = false;

  // location to dump if you want to look at results afterward (null value results in no writing)
  private static final String OUTPUT_FILE_LOCATION = null; //"/Users/rdoherty/tabular_results.txt";

  @Test
  public void doTallRowsPerfTest() throws Exception {
    Entity entity = TEST_ENTITY.apply(new MockModel());
    List<String> outputColumns = FilteredResultFactory.getTabularOutputColumns(entity, entity.getVariables());
    TallRowsGeneratedResultIterator iterator = new TallRowsGeneratedResultIterator(entity, NUM_RECORDS_TO_PROCESS, CACHE_SAMPLE_RECORD);
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(OUTPUT_STREAM_PROVIDER.get()))) {
      Timer t = new Timer();
      FilteredResultFactory.writeWideRowsFromTallResult(iterator, TabularResponses.Type.TABULAR.getFormatter().getFormatter(writer), outputColumns, entity, false);
      writer.flush();
      LOG.info("Time to dump " + NUM_RECORDS_TO_PROCESS + " entity records: " + t.getElapsedString());
    }
  }

  private static final SupplierWithException<OutputStream> OUTPUT_STREAM_PROVIDER =
      () -> OUTPUT_FILE_LOCATION == null

      // write to nowhere for general performance testing
      ? new OutputStream() { @Override public void write(int b) throws IOException { } }

      // use local file if you want to look at the results after the test runs
      : new FileOutputStream(OUTPUT_FILE_LOCATION);

}
