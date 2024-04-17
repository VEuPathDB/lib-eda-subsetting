package org.veupathdb.service.eda.subset.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.subset.model.Entity;
import org.veupathdb.service.eda.subset.model.Study;
import org.veupathdb.service.eda.subset.model.StudyOverview;
import org.veupathdb.service.eda.subset.model.StudyOverview.StudySourceType;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.veupathdb.service.eda.subset.model.db.DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME;
import static org.veupathdb.service.eda.subset.model.db.DB.Tables.Study.Columns.STUDY_DATE_MODIFIED_COL_NAME;
import static org.veupathdb.service.eda.subset.model.db.DB.Tables.Study.Columns.STUDY_ID_COL_NAME;

public class StudyFactory implements StudyProvider {

  private static final Logger LOG = LogManager.getLogger(StudyFactory.class);

  private final DataSource _dataSource;
  private final String _dataSchema;
  private final StudySourceType _sourceType;
  private final VariableFactory _variableFactory;
  private final boolean _sortEntities;

  public StudyFactory(DataSource dataSource,
                      String dataSchema,
                      StudySourceType sourceType,
                      VariableFactory variableFactory,
                      boolean sortEntities) {
    _dataSource = dataSource;
    _dataSchema = dataSchema;
    _sourceType = sourceType;
    _variableFactory = variableFactory;
    _sortEntities = sortEntities;
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
        Date lastModified = rs.getDate(3, Calendar.getInstance(TimeZone.getTimeZone("UTC")));
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

    TreeNode<Entity> entityTree = new EntityFactory(_dataSource, _dataSchema, _sortEntities).getStudyEntityTree(studyId);

    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    CollectionFactory collectionFactory = new CollectionFactory(_dataSource, _dataSchema);

    for (Entity entity : entityIdMap.values()) {
      entity.assignVariables(_variableFactory.loadVariables(overview.getInternalAbbrev(), entity));
      if (entity.hasCollections()) {
        LOG.info("Entity " + entity.getId() + " has collections.  Loading them...");
        entity.assignCollections(collectionFactory.loadCollections(entity));
      }
    }

    return new Study(overview, entityTree, entityIdMap);
  }
}
