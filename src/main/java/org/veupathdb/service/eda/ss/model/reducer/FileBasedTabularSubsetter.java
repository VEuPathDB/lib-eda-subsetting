package org.veupathdb.service.eda.ss.model.reducer;

import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.tabular.TabularReportConfig;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;
import org.veupathdb.service.eda.ss.model.variable.Variable;

import java.util.List;

public class FileBasedTabularSubsetter {

  /**
   * Writes to the passed output stream a "tabular" result.  Exact format depends on the passed
   * responseType (JSON string[][] vs true tabular). Each row is a record containing
   * the primary key columns and requested variables of the specified entity.
   *
   * @param study           study context
   * @param outputEntity    entity type to return
   * @param outputVariables variables requested
   * @param filters         filters to apply to create a subset of records
   * @param reportConfig    configuration of this report
   * @param resultConsumer  consumer to which report should be reported
   */
  public static void produceTabularSubset(Study study, Entity outputEntity,
                                          List<Variable> outputVariables, List<Filter> filters,
                                          TabularReportConfig reportConfig,
                                          TabularResponses.ResultConsumer resultConsumer) {
    /**
     * TODO:
     * STEP 1: Construct {@link FilteredValueStream}s from filters.
     * STEP 2: Construct OutputValueStreams from output variables.
     * STEP 3: Generate Tree based on output entity's position in the study's entity diagram.
     * STEP 4: Run a tree-traversal, kicking off file filter processing and merging streams as we go up the tree.
     */
  }
}
