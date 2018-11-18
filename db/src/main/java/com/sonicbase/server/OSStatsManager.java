package com.sonicbase.server;

import com.sonicbase.common.ComObject;

public class OSStatsManager {

  private final OSStatsManagerImpl impl;

  public OSStatsManager(ProServer proServer, DatabaseServer server) {
    this.impl = new OSStatsManagerImpl(proServer, server);
  }

  public ComObject getOSStats(ComObject cobj, boolean replayedCommand) {
    return impl.getOSStats(cobj, replayedCommand);
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

