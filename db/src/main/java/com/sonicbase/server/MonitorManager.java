package com.sonicbase.server;

import com.sonicbase.common.ComObject;


public class MonitorManager {
  private final MonitorManagerImpl impl;

  public MonitorManager(ProServer proServer, DatabaseServer databaseServer) {
    this.impl = new MonitorManagerImpl(proServer, databaseServer);
  }

  public MonitorManagerImpl getImpl() {
    return impl;
  }

  public ComObject registerQueryForStats(ComObject cobj, boolean replayedCommand) {
    return impl.registerQueryForStats(cobj, replayedCommand);
  }

  public ComObject registerStats(ComObject cobj, boolean replayedCommand) {
    return impl.registerStats(cobj, replayedCommand);
  }

  public ComObject initConnection(ComObject cobj, boolean replayedCommand) {
    return impl.initConnection(cobj, replayedCommand);
  }

  public ComObject initMonitoringTables(ComObject cobj, boolean replayedCommand) {
    return impl.initMonitoringTables(cobj, replayedCommand);
  }

  public void shutdown() {
    impl.shutdown();
  }
}
