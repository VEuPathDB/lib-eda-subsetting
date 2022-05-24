package org.veupathdb.service.eda.ss.model.db;

import jakarta.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;

public class StudyFactory {

  private static final Logger LOG = LogManager.getLogger(StudyFactory.class);

  private final DataSource _dataSource;
  private final String _appDbSchema;
  private final boolean _convertAssaysFlag;

  public StudyFactory(DataSource dataSource, String appDbSchema, boolean convertAssaysFlag) {
    _dataSource = dataSource;
    _appDbSchema = appDbSchema;
    _convertAssaysFlag = convertAssaysFlag;
  }

  public List<StudyOverview> getStudyOverviews() {
    String sql = getStudyOverviewSql(null, _appDbSchema);
    return new SQLRunner(_dataSource, sql, "Get list of study overviews").executeQuery(rs -> {
      List<StudyOverview> studyOverviews = new ArrayList<>();
      while (rs.next()) {
        String id = rs.getString(1);
        String abbrev = rs.getString(2);
        StudyOverview study = new StudyOverview(id, abbrev);
        studyOverviews.add(study);
      }
      return studyOverviews;
    });
  }

  public Study loadStudy(String studyId) {

    StudyOverview overview =
        getStudyOverview(studyId).orElseThrow(() -> new NotFoundException("Study ID '" + studyId + "' not found:"));

    TreeNode<Entity> entityTree = new EntityFactory(_dataSource, _appDbSchema, _convertAssaysFlag).getStudyEntityTree(studyId);

    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    VariableFactory variableFactory = new VariableFactory(_dataSource, _appDbSchema);
    CollectionFactory collectionFactory = new CollectionFactory(_dataSource, _appDbSchema);

    for (Entity entity : entityIdMap.values()) {
      entity.assignVariables(variableFactory.loadVariables(entity));
      if (entity.hasCollections()) {
        LOG.info("Entity " + entity.getId() + " has collections.  Loading them...");
        entity.assignCollections(collectionFactory.loadCollections(entity));
      }
    }

    return new Study(overview, entityTree, entityIdMap);
  }

  public Optional<StudyOverview> getStudyOverview(String studyId) {
    String sql = getStudyOverviewSql(studyId, _appDbSchema);

    return new SQLRunner(_dataSource, sql, "Get study overview").executeQuery(rs -> {
      if (!rs.next()) return Optional.empty();
      String id = rs.getString(1);
      String abbrev = rs.getString(2);
      return Optional.of(new StudyOverview(id, abbrev));
    });
  }

  // studyId is optional. if provided, constrain returned studies to that one study id.
  private static String getStudyOverviewSql(String studyId, String appDbSchema) {
    String whereClause = "";
    if (studyId != null) whereClause = " where s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + " = '" + studyId + "'";
    return
        "select s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME +
            ", s." + DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME +
            " from " + appDbSchema + DB.Tables.Study.NAME + " s " +
            whereClause;
  }
}
