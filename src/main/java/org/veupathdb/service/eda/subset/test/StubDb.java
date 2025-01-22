package org.veupathdb.service.eda.subset.test;

import org.gusdb.fgputil.db.SqlScriptRunner;
import org.hsqldb.jdbc.JDBCDataSource;
import org.veupathdb.service.eda.subset.model.StudyOverview;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

public class StubDb {

  private static final String DB_SCHEMA_SCRIPT = "org/veupathdb/service/eda/subset/stubdb/createDbSchema.sql";
  private static final String DB_DATA_SCRIPT = "org/veupathdb/service/eda/subset/stubdb/insertDbData.sql";

  private static final String STUB_DB_NAME = "stubDb";

  // empty schema for test DB
  public static final String APP_DB_SCHEMA = "";

  // tests run against non-user studies
  public static final StudyOverview.StudySourceType USER_STUDIES_FLAG = StudyOverview.StudySourceType.CURATED;

  private static volatile DataSource _ds;

  public static DataSource getDataSource() {
    if (_ds == null) {
      synchronized(StubDb.class) {
        if (_ds == null) _ds = loadDataSource();
      }
    }
    return _ds;
  }

  private static DataSource loadDataSource() {
    try {
      JDBCDataSource ds = new JDBCDataSource();
      ds.setDatabase("jdbc:hsqldb:mem:" + STUB_DB_NAME);
      ds.setUser("stubby");
      ds.setPassword("");
      SqlScriptRunner.runSqlScript(ds, DB_SCHEMA_SCRIPT);
      SqlScriptRunner.runSqlScript(ds, DB_DATA_SCRIPT);
      return ds;
    }
    catch (SQLException | IOException e) {
      throw new RuntimeException("Unable to load stud database", e);
    }
  }
}
