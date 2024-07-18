package org.veupathdb.service.eda.subset.model.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

public class PlatformUtils {
  private static final Logger LOG = LogManager.getLogger(PlatformUtils.class);

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
    try (Connection c = dataSource.getConnection()) {
      final String productName = c.getMetaData().getDatabaseProductName();
      LOG.info("Found product name: " + productName);
      return Arrays.stream(DBPlatform.values())
          .filter(platform -> platform.getProductName().equals(productName))
          .findAny()
          .orElse(DBPlatform.Other);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
