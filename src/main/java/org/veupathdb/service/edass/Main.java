package org.veupathdb.service.edass;

import org.gusdb.fgputil.db.slowquery.QueryLogConfig;
import org.gusdb.fgputil.db.slowquery.QueryLogger;
import org.veupathdb.lib.container.jaxrs.config.Options;
import org.veupathdb.lib.container.jaxrs.server.ContainerResources;
import org.veupathdb.lib.container.jaxrs.server.Server;

public class Main extends Server {
  public static void main(String[] args) {
    QueryLogger.initialize(new QueryLogConfig(){});
    new Main().start(args);
  }

  @Override
  protected ContainerResources newResourceConfig(Options options) {
    final var out =  new Resources(options);

    // Enabled by default for debugging purposes, this should be removed when
    // production ready.
    out.enableJerseyTrace();

    return out;
  }
}
