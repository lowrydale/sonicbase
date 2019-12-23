/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.query.DatabaseException;

import java.sql.SQLException;
import java.text.NumberFormat;

class BulkImportHandler {

  public static final String ERROR_NOT_USING_A_CLUSTER_STR = "Error, not using a cluster";
  public static final String ERROR_NOT_USING_A_DATABASE_STR = "Error, not using a database";
  private final Cli cli;

  BulkImportHandler(Cli cli) {
    this.cli = cli;
  }

  //bulk import status
  void bulkImportStatus() throws SQLException, ClassNotFoundException {
    cli.initConnection();

    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    ComObject cobj = new ComObject(2);
    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:getBulkImportProgress");
    cobj.put(ComObject.Tag.DB_NAME, cli.getCurrDbName());
    byte[] bytes = cli.getConn().sendToMaster(cobj);
    ComObject retObj = new ComObject(bytes);

    ComArray array = retObj.getArray(ComObject.Tag.PROGRESS_ARRAY);
    for (int i = 0; i < array.getArray().size(); i++) {
      ComObject tableObj = (ComObject) array.getArray().get(i);
      String tableName = tableObj.getString(ComObject.Tag.TABLE_NAME);
      long countProcessed = tableObj.getLong(ComObject.Tag.COUNT_LONG);
      long expectedCount = tableObj.getLong(ComObject.Tag.EXPECTED_COUNT);
      boolean finished = tableObj.getBoolean(ComObject.Tag.FINISHED);
      long preProcessCountProcessed = tableObj.getLong(ComObject.Tag.PRE_PROCESS_COUNT_PROCESSED);
      long preProcessExpectedCount = tableObj.getLong(ComObject.Tag.PRE_PROCESS_EXPECTED_COUNT);
      boolean preProcessFinished = tableObj.getBoolean(ComObject.Tag.PRE_PROCESS_FINISHED);
      if (!preProcessFinished) {
        cli.println(String.format("preprocessing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(preProcessCountProcessed),
            (double) preProcessCountProcessed / (double) preProcessExpectedCount * 100d, preProcessFinished));
        if (tableObj.getString(ComObject.Tag.PRE_PROCESS_EXCEPTION) != null) {
          cli.println(tableObj.getString(ComObject.Tag.PRE_PROCESS_EXCEPTION));
        }
      }
      else {
        cli.println(String.format("processing table=%s, countFinished=%s, percentComplete=%.2f, finished=%b",
            tableName, NumberFormat.getIntegerInstance().format(countProcessed),
            (double) countProcessed / (double) expectedCount * 100d, finished));
        if (tableObj.getString(ComObject.Tag.EXCEPTION) != null) {
          cli.println(tableObj.getString(ComObject.Tag.EXCEPTION));
        }
      }
    }
  }

  //start bulk import from persons(com.sonicbase.jdbcdriver.Driver, jdbc:sonicbase:localhost:9010/db, <user>, <password>) where <expression>
  void startBulkImport(String command) throws SQLException, ClassNotFoundException {
    cli.initConnection();

    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    ComObject cobj = new ComObject(6);
    cobj.put(ComObject.Tag.DB_NAME, cli.getCurrDbName());
    int pos = command.indexOf("from");
    int pos1 = command.indexOf('(', pos);
    String tableNames = command.substring(pos + "from".length(), pos1).trim();
    cobj.put(ComObject.Tag.TABLE_NAME, tableNames);

    pos = command.indexOf(", ", pos1);
    String driverName = command.substring(pos1 + 1, pos).trim();
    cobj.put(ComObject.Tag.DRIVER_NAME, driverName);
    pos1 = command.indexOf(", ", pos + 1);
    int pos2 = command.indexOf(')', pos1);
    String jdbcUrl = command.substring(pos + 1, pos1 == -1 ? pos2 : pos1).trim();
    cobj.put(ComObject.Tag.CONNECT_STRING, jdbcUrl);
    String user = null;
    String password = null;
    int endParenPos = pos2;
    if (pos1 != -1) {
      //has user/password
      pos = command.indexOf(", ", pos1);
      user = command.substring(pos1 + 1, pos).trim();
      endParenPos = command.indexOf(')', pos);
      password = command.substring(pos + 1, endParenPos).trim();
      cobj.put(ComObject.Tag.USER, user);
      cobj.put(ComObject.Tag.PASSWORD, password);
    }
    String whereClause = command.substring(endParenPos + 1).trim();
    if (whereClause.length() != 0) {
      if (tableNames.contains(",")) {
        throw new DatabaseException("You cannot have a where clause with multiple tables");
      }
      cobj.put(ComObject.Tag.WHERE_CLAUSE, whereClause);
    }

    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:startBulkImport");

    cli.getConn().sendToMaster(cobj);
  }

  void cancelBulkImport() {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    ComObject cobj = new ComObject(2);
    cobj.put(ComObject.Tag.METHOD, "BulkImportManager:cancelBulkImport");
    cobj.put(ComObject.Tag.DB_NAME, cli.getCurrDbName());
    for (int i = 0; i < cli.getConn().getShardCount(); i++) {
      for (int j = 0; j < cli.getConn().getReplicaCount(); j++) {
        cli.getConn().send(null, i, j, cobj, ConnectionProxy.Replica.SPECIFIED);
      }
    }
  }

}
