package org.veupathdb.service.eda.subset.model.db;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Arrays;

public class PlatformUtils {

  public enum DBPlatform {
    PostgresDB("PostgreSQL"),
    Oracle("Oracle"),
    Other("");

    final String productName;

    public String getProductName() {
      return productName;
    }

    DBPlatform(String productName) {
      this.productName = productName;
    }
  }

  public static DBPlatform fromDataSource(DataSource dataSource) {
    try {
      final String productName = dataSource.getConnection().getMetaData().getDatabaseProductName();
      return Arrays.stream(DBPlatform.values())
          .filter(platform -> platform.getProductName().equals(productName))
          .findAny()
          .orElse(DBPlatform.Other);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
