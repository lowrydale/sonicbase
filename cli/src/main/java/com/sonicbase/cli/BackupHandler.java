package com.sonicbase.cli;

import com.sonicbase.common.ComObject;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;


public class BackupHandler {

  private static Logger logger = LoggerFactory.getLogger(BackupHandler.class);
  private final Cli cli;

  public BackupHandler(Cli cli) {
    this.cli = cli;
  }

  public void startBackup() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    cli.getConn().startBackup();
  }

  public void startRestore(String command) throws SQLException, ClassNotFoundException, IOException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
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

  public void backupStatus() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    if (cli.getConn().isBackupComplete()) {
      System.out.println("complete");
    }
    else {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
      ComObject retObj = new ComObject(cli.getConn().sendToMaster("BackupManager:getBackupStatus", cobj));
      double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
      Boolean error = retObj.getBoolean(ComObject.Tag.ERROR);
      percentComplete *= 100d;
      String formatted = String.format("%.2f", percentComplete);
      System.out.println("running - percentComplete=" + formatted + (error != null && error ? ", error=true" : ""));
    }

  }

  public void restoreStatus() throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    if (cli.getConn().isRestoreComplete()) {
      System.out.println("complete");
    }
    else {
      ComObject cobj = new ComObject();
      cobj.put(ComObject.Tag.DB_NAME, "__none__");
      cobj.put(ComObject.Tag.SCHEMA_VERSION, cli.getConn().getSchemaVersion());
      ComObject retObj = new ComObject(cli.getConn().sendToMaster("BackupManager:getRestoreStatus", cobj));

      Boolean error = retObj.getBoolean(ComObject.Tag.ERROR);
      double percentComplete = retObj.getDouble(ComObject.Tag.PERCENT_COMPLETE);
      percentComplete *= 100;
      String formatted = String.format("%.2f", percentComplete);

      String stage = retObj.getString(ComObject.Tag.STAGE);
      if (stage.equals("copyingFiles")) {
        System.out.println("running - stage=copyingFiles, percentComplete=" + formatted +  (error != null && error ? ", error=true" : ""));
      }
      else {
        System.out.println("running - stage=" + stage + ", percentComplete=" + formatted);
      }
    }
  }


}
