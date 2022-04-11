package org.veupathdb.service.eda.ss.model.db;

import jakarta.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.functional.TreeNode;
import org.veupathdb.service.eda.ss.Resources;
import org.veupathdb.service.eda.ss.model.Entity;
import org.veupathdb.service.eda.ss.model.Study;
import org.veupathdb.service.eda.ss.model.StudyOverview;

public class StudyFactory {

  private final DataSource _dataSource;

  public StudyFactory(DataSource dataSource) {
    _dataSource = dataSource;
  }

  public List<StudyOverview> getStudyOverviews() {
    String sql = getStudyOverviewSql(null);
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

  /*
   * Expects a pre-validated study ID
   */
  public Study loadStudy(String studyId) {

    StudyOverview overview =
        getStudyOverview(studyId).orElseThrow(() -> new NotFoundException("Study ID '" + studyId + "' not found:"));

    TreeNode<Entity> entityTree = new EntityFactory(_dataSource).getStudyEntityTree(studyId);

    Map<String, Entity> entityIdMap = entityTree.flatten().stream().collect(Collectors.toMap(Entity::getId, e -> e));

    VariableFactory variableFactory = new VariableFactory(_dataSource);
    CollectionFactory collectionFactory = new CollectionFactory(_dataSource);

    for (Entity entity : entityIdMap.values()) {
      entity.assignVariables(variableFactory.loadVariables(entity));
      if (entity.hasCollections()) {
        entity.assignCollections(collectionFactory.loadCollections(entity));
      }
    }

    return new Study(overview, entityTree, entityIdMap);
  }

  public Optional<StudyOverview> getStudyOverview(String studyId) {
    String sql = getStudyOverviewSql(studyId);

    return new SQLRunner(_dataSource, sql, "Get study overview").executeQuery(rs -> {
      if (!rs.next()) return Optional.empty();
      String id = rs.getString(1);
      String abbrev = rs.getString(2);
      return Optional.of(new StudyOverview(id, abbrev));
    });
  }

  // studyId is optional. if provided, constrain returned studies to that one study id.
  private static String getStudyOverviewSql(String studyId) {
    String whereClause = "";
    if (studyId != null) whereClause = " where s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME + " = '" + studyId + "'";
    return
        "select s." + DB.Tables.Study.Columns.STUDY_ID_COL_NAME +
            ", s." + DB.Tables.Study.Columns.STUDY_ABBREV_COL_NAME +
            " from " + Resources.getAppDbSchema() + DB.Tables.Study.NAME + " s " +
            whereClause;
  }
}
