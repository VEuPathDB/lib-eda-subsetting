package org.veupathdb.service.edass;

import org.veupathdb.lib.container.jaxrs.config.Options;
import org.veupathdb.lib.container.jaxrs.server.ContainerResources;
import org.veupathdb.lib.container.jaxrs.utils.db.DbManager;
import org.veupathdb.service.edass.service.Studies;
import org.veupathdb.service.edass.stubdb.StubDb;

import javax.sql.DataSource;

/**
 * Service Resource Registration.
 *
 * This is where all the individual service specific resources and middleware
 * should be registered.
 */
public class Resources extends ContainerResources {

  private static final boolean USE_IN_MEMORY_TEST_DATABASE = true;

  public Resources(Options opts) {
    super(opts);
    if (!USE_IN_MEMORY_TEST_DATABASE)
      DbManager.initApplicationDatabase(opts);
  }

  /**
   * Returns an array of JaxRS endpoints, providers, and contexts.
   *
   * Entries in the array can be either classes or instances.
   */
  @Override
  protected Object[] resources() {
    return new Object[] {
      Studies.class,
    };
  }

  public static DataSource getApplicationDataSource() {
    return USE_IN_MEMORY_TEST_DATABASE
      ? StubDb.getDataSource()
      : DbManager.applicationDatabase().getDataSource();
  }
}
