package com.sonicbase.cli;

import com.sonicbase.common.ComObject;
import com.sonicbase.query.DatabaseException;

import java.sql.SQLException;


class BackupHandler {

  private static final String ERROR_NOT_USING_A_CLUSTER_STR = "Error, not using a cluster";
  private static final String ERROR_STR = ", error=";
  private final Cli cli;

  BackupHandler(Cli cli) {
    this.cli = cli;
  }

  void startBackup() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    cli.initConnection();

    cli.getConn().startBackup();
  }

  void startRestore(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    cli.initConnection();

    String subDir = command.substring(command.indexOf("start restore") + "start restore".length());
    subDir = subDir.trim();
    if (subDir.length() == 0) {
      throw new DatabaseException("You must specify the backup subdirectory you want to restore");
    }

    cli.getConn().startRestore(subDir);
  }

  void backupStatus() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    cli.initConnection();

    if (cli.getConn().isBackupComplete()) {
      cli.println("complete");
    }
    else {
      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
      ComObject retObj = new ComObject(cli.getConn().sendToMaster("BackupManager:getBackupStatus", cobj));
      double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
      String error = retObj.getString(ComObject.Tag.EXCEPTION);
      percentComplete *= 100d;
      String formatted = String.format("%.2f", percentComplete);
      cli.println("running - percentComplete=" + formatted + (error != null ? ERROR_STR + error : ""));
    }

  }

  void restoreStatus() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      cli.println(ERROR_NOT_USING_A_CLUSTER_STR);
      return;
    }

    cli.initConnection();

    if (cli.getConn().isRestoreComplete()) {
      cli.println("complete");
    }
    else {
      ComObject cobj = new ComObject(2);
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
      ComObject retObj = new ComObject(cli.getConn().sendToMaster("BackupManager:getRestoreStatus", cobj));

      String error = retObj.getString(ComObject.Tag.EXCEPTION);
      double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
      percentComplete *= 100;
      String formatted = String.format("%.2f", percentComplete);

      String stage = retObj.getString(ComObject.Tag.STAGE);
      if (stage.equals("copyingFiles")) {
        cli.println("running - stage=copyingFiles, percentComplete=" + formatted +  (error != null ? ERROR_STR + error : ""));
      }
      else {
        cli.println("running - stage=" + stage + ", percentComplete=" + formatted +  (error != null ? ERROR_STR + error : ""));
      }
    }
  }


}
