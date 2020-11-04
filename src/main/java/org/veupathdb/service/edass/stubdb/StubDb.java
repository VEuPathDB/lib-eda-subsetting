package org.veupathdb.service.edass.stubdb;

import org.gusdb.fgputil.db.SqlScriptRunner;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.hsqldb.jdbc.JDBCDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

public class StubDb {

  private static final String DB_SCHEMA_SCRIPT = "org/veupathdb/service/edass/stubdb/createDbSchema.sql";
  private static final String DB_DATA_SCRIPT = "org/veupathdb/service/edass/stubdb/insertDbData.sql";

  private static final String STUB_DB_NAME = "stubDb";

  private static DataSource _ds;

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
