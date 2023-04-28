package org.veupathdb.service.eda.ss.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;
import org.veupathdb.service.eda.ss.model.StudyOverview.StudySourceType;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.veupathdb.service.eda.ss.model.db.DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.Study.Columns.STUDY_DATE_MODIFIED_COL_NAME;
import static org.veupathdb.service.eda.ss.model.db.DB.Tables.Study.Columns.STUDY_ID_COL_NAME;

public class StudyFactory implements StudyProvider {

  private static final Logger LOG = LogManager.getLogger(StudyFactory.class);

  private final DataSource _dataSource;
  private final String _dataSchema;
  private final StudySourceType _sourceType;
  private final VariableFactory _variableFactory;

  public StudyFactory(DataSource dataSource,
                      String dataSchema,
                      StudySourceType sourceType,
                      VariableFactory variableFactory) {
    _dataSource = dataSource;
    _dataSchema = dataSchema;
    _sourceType = sourceType;
    _variableFactory = variableFactory;
  }

  private static String getStudyOverviewSql(String appDbSchema) {
    return "select s." + STUDY_ID_COL_NAME + ", s." + STUDY_ABBREV_COL_NAME + ", s." + STUDY_DATE_MODIFIED_COL_NAME +
           " from " + appDbSchema + DB.Tables.Study.NAME + " s ";
  }

  @Override
  public List<StudyOverview> getStudyOverviews() {
    String sql = getStudyOverviewSql(_dataSchema);
    return new SQLRunner(_dataSource, sql, "Get list of study overviews").executeQuery(rs -> {
      List<StudyOverview> studyOverviews = new ArrayList<>();
      while (rs.next()) {
        String id = rs.getString(1);
        String abbrev = rs.getString(2);
        Date lastModified = rs.getDate(3);
        StudyOverview study = new StudyOverview(id, abbrev, _sourceType, lastModified);
        studyOverviews.add(study);
      }
      return studyOverviews;
    });
  }

  @Override
  public Study getStudyById(String studyId) {

    StudyOverview overview = getStudyOverviews().stream()
      .filter(study -> study.getStudyId().equals(studyId))
      .findFirst()
      .orElseThrow(notFound(studyId));

    TreeNode<Entity> entityTree = new EntityFactory(_dataSource, _dataSchema).getStudyEntityTree(studyId);

    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    CollectionFactory collectionFactory = new CollectionFactory(_dataSource, _dataSchema);

    for (Entity entity : entityIdMap.values()) {
      entity.assignVariables(_variableFactory.loadVariables(studyId, entity));
      if (entity.hasCollections()) {
        LOG.info("Entity " + entity.getId() + " has collections.  Loading them...");
        entity.assignCollections(collectionFactory.loadCollections(entity));
      }
    }

    return new Study(overview, entityTree, entityIdMap);
  }
}
