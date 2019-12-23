/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.sonicbase.jdbcdriver.ResultSetProxy;
import com.sonicbase.query.impl.ResultSetImpl;
import com.sonicbase.schema.DataType;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

class DescribeHandler {

  public static final String ERROR_NOT_USING_A_DATABASE_STR = "Error, not using a database";
  public static final String ADDRESS_STR = "Address";
  public static final String SHARD_STR = "shard";
  public static final String REPLICA_STR = "replica";
  private static ResultSet describeSchemaVersionRet;
  private static ResultSet describeServerStatsRet;
  private static ResultSet ret;
  private static int nextStatsOffset;
  private static ResultSet describeServerHealthRet;
  private final Cli cli;

  DescribeHandler(Cli cli) {
    this.cli = cli;
  }

  private void getPartitionSizes(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {

    describeShards(command);
  }

  private void describeShards(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    ret = stmt.executeQuery();
    cli.setRet(ret);

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1].trim();

    int currLine = 0;
    cli.println("");
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++, currLine++) {
      if (!ret.next()) {
        break;
      }
      cli.println(ret.getString(1));
    }
    if (!ret.isLast()) {
      cli.println("next");
    }
    cli.setLastCommand(command);

  }

  private void describeRepartitioner(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    cli.setRet(stmt.executeQuery());

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1].trim();

    int currLine = 0;
    cli.println("");
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++, currLine++) {
      if (!ret.next()) {
        break;
      }
      cli.println(ret.getString(1));
    }
    if (!ret.isLast()) {
      cli.println("next");
    }
    cli.setLastCommand(command);
  }

  void describe(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    if ("describe shards".equals(command)) {
      getPartitionSizes(command);
      return;
    }
    else if ("describe repartitioner".equals(command)) {
      describeRepartitioner(command);
      return;
    }
    else if ("describe server stats".equals(command)) {
      describeServerStats(command);
      return;
    }
    else if ("describe server health".equals(command)) {
      describeServerHealth(command);
      return;
    }
    else if ("describe schema version".equals(command)) {
      describeSchemaVersion(command);
      return;
    }

    cli.initConnection();

    cli.println("Executing select request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    ret = stmt.executeQuery();
    cli.setRet(ret);

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1].trim();

    int currLine = 0;
    cli.println("");
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++, currLine++) {
      if (!ret.next()) {
        break;
      }
      cli.println(ret.getString(1));
    }
    cli.setLastCommand(command);
  }

  private void describeServerStats(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);

    describeServerStatsRet = stmt.executeQuery();

    displayServerStatsPage(0);

    cli.setLastCommand(command);
  }

  void displayServerStatsPage(int offset) throws IOException, InterruptedException, SQLException {


    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn(ADDRESS_STR, ""));
    columns.add(new Cli.SelectColumn("CPU", "%"));
    columns.add(new Cli.SelectColumn("ResMem", "Gig"));
    columns.add(new Cli.SelectColumn("JMemMin", "Gig"));
    columns.add(new Cli.SelectColumn("JMemMax", "Gig"));
    columns.add(new Cli.SelectColumn("NetIn", "GByte"));
    columns.add(new Cli.SelectColumn("NetOut", "GByte"));
    columns.add(new Cli.SelectColumn("Disk", "Gig"));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0].trim();
    String height = parts[1].trim();

    List<List<String>> data = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(height) - 8; i++) {
      if (describeServerStatsRet.next()) {
        List<String> row = new ArrayList<>();
        row.add(describeServerStatsRet.getString("host"));
        row.add(describeServerStatsRet.getString("cpu"));
        row.add(describeServerStatsRet.getString("resGig"));
        row.add(describeServerStatsRet.getString("javaMemMin"));
        row.add(describeServerStatsRet.getString("javaMemMax"));
        row.add(describeServerStatsRet.getString("receive"));
        row.add(describeServerStatsRet.getString("transmit"));
        row.add(describeServerStatsRet.getString("diskAvail"));
        data.add(row);
      }
      else {
        break;
      }
    }

    cli.displayPageOfData(columns, width, data, describeServerStatsRet.isLast());

    nextStatsOffset = offset;
  }

  private void describeServerHealth(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    describeServerHealthRet = stmt.executeQuery();

    displayServerHealthPage(0);

    cli.setLastCommand(command);
  }

  void displayServerHealthPage(int offset) throws IOException, InterruptedException, SQLException {
    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn(ADDRESS_STR, ""));
    columns.add(new Cli.SelectColumn("Shard", ""));
    columns.add(new Cli.SelectColumn("Replica", ""));
    columns.add(new Cli.SelectColumn("Dead", ""));
    columns.add(new Cli.SelectColumn("Master", ""));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0].trim();
    String height = parts[1].trim();

    List<List<String>> data = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(height) - 8; i++) {
      if (describeServerHealthRet.next()) {
        List<String> row = new ArrayList<>();
        row.add(describeServerHealthRet.getString("host"));
        row.add(describeServerHealthRet.getString(SHARD_STR));
        row.add(describeServerHealthRet.getString(REPLICA_STR));
        row.add(describeServerHealthRet.getString("dead"));
        row.add(describeServerHealthRet.getString("master"));
        data.add(row);
      }
      else {
        break;
      }
    }

    cli.displayPageOfData(columns, width, data, describeServerHealthRet.isLast());

    nextStatsOffset = offset;
  }


  private void describeSchemaVersion(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    describeSchemaVersionRet = stmt.executeQuery();

    displaySchemaVersionPage(0);

    cli.setLastCommand(command);
  }

  private void displaySchemaVersionPage(int offset) throws IOException, InterruptedException, SQLException {
    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn(ADDRESS_STR, ""));
    columns.add(new Cli.SelectColumn("Shard", ""));
    columns.add(new Cli.SelectColumn("Replica", ""));
    columns.add(new Cli.SelectColumn("Version", ""));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0].trim();
    String height = parts[1].trim();

    List<List<String>> data = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(height) - 8; i++) {
      if (describeSchemaVersionRet.next()) {
        List<String> row = new ArrayList<>();
        row.add(describeSchemaVersionRet.getString("host"));
        row.add(describeSchemaVersionRet.getString(SHARD_STR));
        row.add(describeSchemaVersionRet.getString(REPLICA_STR));
        row.add(describeSchemaVersionRet.getString("version"));
        data.add(row);
      }
      else {
        break;
      }
    }

    cli.displayPageOfData(columns, width, data, describeSchemaVersionRet.isLast());

    nextStatsOffset = offset;
  }

  void describeLicenses() throws IOException, SQLException, InterruptedException, ClassNotFoundException {

    cli.initConnection();

    ret = new ResultSetProxy((ResultSetImpl) cli.getConn().describeLicenses());
    cli.setRet(ret);
    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1].trim();

    int currLine = 0;
    cli.println("");
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++, currLine++) {
      if (!ret.next()) {
        break;
      }
      cli.println(ret.getString(1));
    }
    if (!ret.isLast()) {
      cli.println("next");
    }
    cli.setLastCommand(cli.getCommand());
  }

}
