package org.veupathdb.service.eda.ss.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.tabular.TabularResponses;
import org.veupathdb.service.eda.ss.model.variable.VariableWithValues;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;

public class StudyVocabHandler {
  private static final Logger LOG = LogManager.getLogger(StudyVocabHandler.class);

  // TODO Cache vocabs.

  public void queryStudyVocab(String schema,
                              DataSource dataSource,
                              Entity studyEntity,
                              VariableWithValues vocabularyVariable,
                              TabularResponses.ResultConsumer resultConsumer) {
    new SQLRunner(dataSource, getSqlForVocabQuery(schema, vocabularyVariable.getEntity(), studyEntity, vocabularyVariable)).executeQuery(rs -> {
      int numConsumed = 0;
      try {
        resultConsumer.begin();
        while (rs.next()) {
          try {
            numConsumed++;
            final String studyId = rs.getString(studyEntity.getPKColName());
            final String value = vocabularyVariable.getType().convertRowValueToStringValue(rs);
            if (numConsumed > 5000) {
              LOG.info("Consuming row: {},{}", studyId, value);
            }
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

  private String getSqlForVocabQuery(String appDbSchema, Entity outputEntity, Entity studyEntity, VariableWithValues vocabVar) {
    final String attributeValTable = appDbSchema + DB.Tables.AttributeValue.NAME(outputEntity);
    return "SELECT DISTINCT " + studyEntity.getPKColName() + ", string_value, number_value, date_value FROM " + attributeValTable + " value \n" +
        " JOIN " + appDbSchema + DB.Tables.Ancestors.NAME(outputEntity) + " ancestors \n" +
        " ON ancestors." + outputEntity.getPKColName() + " = value." + outputEntity.getPKColName() + " \n " +
        " WHERE attribute_stable_id = '" + vocabVar.getId() + "' \n " +
        " ORDER BY " + studyEntity.getPKColName();
  }
}