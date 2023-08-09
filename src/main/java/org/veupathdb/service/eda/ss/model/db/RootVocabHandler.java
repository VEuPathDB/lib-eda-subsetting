package org.veupathdb.service.eda.ss.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.filter.Filter;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.gusdb.fgputil.FormatUtil.NL;

/**
 * Business logic class for retrieving vocabularies per study.
 */
public class RootVocabHandler {
  private static final Logger LOG = LogManager.getLogger(RootVocabHandler.class);

  // TODO Cache vocabs.

  /**
   * Query the distinct vocab per study in a megastudy and stream the results to the client.
   *
   * @param schema Database schema containing the study.
   * @param dataSource Database connection.
   * @param studyEntity Entity of the "study" entity within the megastudy.
   * @param vocabularyVariable Variable for which we want to know each distinct value within each study.
   * @param resultConsumer Consumer to stream the results of the query.
   */
  public void queryStudyVocab(String schema,
                              DataSource dataSource,
                              Entity studyEntity,
                              VariableWithValues vocabularyVariable,
                              TabularResponses.ResultConsumer resultConsumer,
                              List<Filter> filters) {
    List<Filter> vocabFilters = filters.stream()
        .filter(filter -> filter.filtersOnVariable(vocabularyVariable))
        .collect(Collectors.toList());
    new SQLRunner(dataSource, getSqlForVocabQuery(schema, vocabularyVariable.getEntity(), studyEntity, vocabularyVariable, vocabFilters)).executeQuery(rs -> {
      try {
        resultConsumer.begin();
        while (rs.next()) {
          try {
            final String studyId = rs.getString(studyEntity.getPKColName());
            final String value = vocabularyVariable.getType().convertRowValueToStringValue(rs);
            resultConsumer.consumeRow(List.of(studyId, value));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        resultConsumer.end();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
  }

  private String getSqlForVocabQuery(String appDbSchema,
                                     Entity outputEntity,
                                     Entity studyEntity,
                                     VariableWithValues vocabVar,
                                     List<Filter> filters) {
    final String attributeValTable = appDbSchema + DB.Tables.AttributeValue.NAME(outputEntity);
    List<String> selectColsList = new ArrayList<>(outputEntity.getAncestorPkColNames());
    selectColsList.add(outputEntity.getPKColName());
    String selectCols = String.join(", ", selectColsList);

    // default WITH body assumes no filters.
    String withBody = "  SELECT " + selectCols + " FROM " + appDbSchema + DB.Tables.Ancestors.NAME(outputEntity) + NL;

    if (!filters.isEmpty()) {
      withBody = filters.stream().map(Filter::getSql).collect(Collectors.joining("INTERSECT"));
    }
    return "" +
        " WITH subset AS ( " + withBody + ")" + "\n" +
        " SELECT DISTINCT ancestors." + studyEntity.getPKColName() + ", value.string_value, value.number_value, value.date_value FROM " + attributeValTable + " value \n" +
        " JOIN " + appDbSchema + DB.Tables.Ancestors.NAME(outputEntity) + " ancestors \n" +
        " ON ancestors." + outputEntity.getPKColName() + " = value." + outputEntity.getPKColName() + " \n " +
        " JOIN subset ON subset." + outputEntity.getPKColName() + " = value." + outputEntity.getPKColName() + "\n" +
        " WHERE value.attribute_stable_id = '" + vocabVar.getId() + "' \n " +
        " ORDER BY ancestors." + studyEntity.getPKColName();
  }
}
