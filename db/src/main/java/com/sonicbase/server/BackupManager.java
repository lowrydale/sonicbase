package com.sonicbase.server;

import com.sonicbase.common.ComObject;

import java.util.Map;

public class BackupManager {

  private final BackupManagerImpl impl;

  public BackupManager(ProServer proServer, com.sonicbase.server.DatabaseServer server) {
    this.impl = new BackupManagerImpl(proServer, server);
  }

  public void setBackupConfig(Map<String, Object> backupConfig) {
    this.impl.setBackupConfig(backupConfig);
  }

  public ComObject isFileRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return impl.isFileRestoreComplete(cobj, replayedCommand);
  }

  public ComObject prepareDataFromRestore(ComObject cobj, boolean replayedCommand) {
    return impl.prepareDataFromRestore(cobj, replayedCommand);
  }

  public ComObject prepareForBackup(ComObject cobj, boolean replayedCommand) {
    return impl.prepareForBackup(cobj, replayedCommand);
  }

  public ComObject getBackupStatus(final ComObject cobj, boolean replayedCommand) {
    return impl.getBackupStatus(cobj);
  }

  public ComObject doGetBackupSizes(final ComObject cobj, boolean replayedCommand) {
    return impl.doGetBackupSizes(cobj);
  }

  public ComObject getRestoreStatus(final ComObject cobj, boolean replayedCommand) {
    return impl.getRestoreStatus(cobj);
  }

  public ComObject doGetRestoreSizes(final ComObject cobj, boolean replayedCommand) {
    return impl.doGetRestoreSizes(cobj);
  }

  public ComObject doBackupFileSystem(final ComObject cobj, boolean replayedCommand) {
    return impl.doBackupFileSystem(cobj, replayedCommand);
  }

  public ComObject doBackupAWS(final ComObject cobj, boolean replayedCommand) {
    return impl.doBackupAWS(cobj, replayedCommand);
  }

  public ComObject isBackupComplete(ComObject cobj, boolean replayedCommand) {
    return impl.isBackupComplete(cobj, replayedCommand);
  }

  public ComObject finishBackup(ComObject cobj, boolean replayedCommand) {
    return impl.finishBackup(cobj, replayedCommand);
  }

  public ComObject isEntireBackupComplete(ComObject cobj, boolean replayedCommand) {
    return impl.isEntireBackupComplete(cobj, replayedCommand);
  }

  public byte[] startBackup(ComObject cobj, boolean replayedCommand) {
    return impl.startBackup(cobj, replayedCommand);
  }

  public ComObject getLastBackupDir(ComObject cobj, boolean replayedCommand) {
    return impl.getLastBackupDir(cobj, replayedCommand);
  }

  public ComObject prepareForRestore(ComObject cobj, boolean replayedCommand) {
    return impl.prepareForRestore(cobj, replayedCommand);
  }

  public ComObject doRestoreFileSystem(final ComObject cobj, boolean replayedCommand) {
    return impl.doRestoreFileSystem(cobj, replayedCommand);
  }

  public ComObject doRestoreAWS(final ComObject cobj, boolean replayedCommand) {
    return impl.doRestoreAWS(cobj, replayedCommand);
  }

  public ComObject isRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return impl.isRestoreComplete(cobj, replayedCommand);
  }

  public ComObject finishRestore(ComObject cobj, boolean replayedCommand) {
    return impl.finishRestore(cobj, replayedCommand);
  }

  public ComObject isEntireRestoreComplete(ComObject cobj, boolean replayedCommand) {
    return impl.isEntireRestoreComplete(cobj, replayedCommand);
  }

  public ComObject startRestore(final ComObject cobj, boolean replayedCommand) {
    return impl.startRestore(cobj, replayedCommand);
  }

  public void shutdown() {
    impl.shutdown();
  }
}
