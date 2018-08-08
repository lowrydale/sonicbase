/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import com.mashape.unirest.http.exceptions.UnirestException;
import com.sonicbase.jdbcdriver.ConnectionProxy;
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

public class DescribeHandler {

  private static ResultSet describeSchemaVersionRet;
  private static ResultSet describeServerStatsRet;
  private static ResultSet ret;
  private static int nextStatsOffset;
  private static ResultSet describeServerHealthRet;
  private final Cli cli;

  public DescribeHandler(Cli cli) {
    this.cli = cli;
  }

  public void getPartitionSizes(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {

    describeShards(command);
  }

  private void describeShards(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    ret = stmt.executeQuery();

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    if (!ret.isLast()) {
      System.out.println("next");
    }
    cli.setLastCommand(command);

  }

  public void describeRepartitioner(String command) throws IOException, SQLException, ClassNotFoundException, InterruptedException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    ret = stmt.executeQuery();

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    if (!ret.isLast()) {
      System.out.println("next");
    }
    cli.setLastCommand(command);
  }

  public void describe(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
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

    System.out.println("Executing select request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    ret = stmt.executeQuery();

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 2; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    cli.setLastCommand(command);
  }

  public void describeServerStats(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);

    describeServerStatsRet = stmt.executeQuery();

    displayServerStatsPage(0);

    cli.setLastCommand(command);
  }

  public void displayServerStatsPage(int offset) throws IOException, InterruptedException, SQLException {


    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn("Address", "", "host", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("CPU", "%", "cpu", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("ResMem", "Gig", "resGig", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("JMemMin", "Gig", "javaMemMin", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("JMemMax", "Gig", "javaMemMax", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("NetIn", "GByte", "receive", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("NetOut", "GByte", "transmit", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Disk", "Gig", "diskAvail", DataType.Type.VARCHAR));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0];
    String height = parts[1];

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

  public void describeServerHealth(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    describeServerHealthRet = stmt.executeQuery();

    displayServerHealthPage(0);

    cli.setLastCommand(command);
  }

  public void displayServerHealthPage(int offset) throws IOException, InterruptedException, SQLException {
    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn("Address", "", "host", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Shard", "", "shard", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Replica", "", "replica", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Dead", "", "dead", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Master", "", "master", DataType.Type.VARCHAR));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0];
    String height = parts[1];

    List<List<String>> data = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(height) - 8; i++) {
      if (describeServerHealthRet.next()) {
        List<String> row = new ArrayList<>();
        row.add(describeServerHealthRet.getString("host"));
        row.add(describeServerHealthRet.getString("shard"));
        row.add(describeServerHealthRet.getString("replica"));
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


  public void describeSchemaVersion(String command) throws SQLException, ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    describeSchemaVersionRet = stmt.executeQuery();

    displaySchemaVersionPage(0);

    cli.setLastCommand(command);
  }

  private void displaySchemaVersionPage(int offset) throws IOException, InterruptedException, SQLException {
    List<Cli.SelectColumn> columns = new ArrayList<>();
    columns.add(new Cli.SelectColumn("Address", "", "host", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Shard", "", "shard", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Replica", "", "replica", DataType.Type.VARCHAR));
    columns.add(new Cli.SelectColumn("Version", "", "version", DataType.Type.VARCHAR));

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String width = parts[0];
    String height = parts[1];

    List<List<String>> data = new ArrayList<>();
    for (int i = 0; i < Integer.valueOf(height) - 8; i++) {
      if (describeSchemaVersionRet.next()) {
        List<String> row = new ArrayList<>();
        row.add(describeSchemaVersionRet.getString("host"));
        row.add(describeSchemaVersionRet.getString("shard"));
        row.add(describeSchemaVersionRet.getString("replica"));
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

  public void describeLicenses() throws IOException, UnirestException, SQLException, InterruptedException, ClassNotFoundException {

    ret = new ResultSetProxy((ResultSetImpl) ConnectionProxy.describeLicenses());

    String str = cli.getTerminalSize();
    String[] parts = str.split(",");
    String height = parts[1];

    int currLine = 0;
    System.out.println();
    for (int i = 0; currLine < Integer.valueOf(height) - 3; i++) {
      if (!ret.next()) {
        break;
      }
      System.out.println(ret.getString(1));
      currLine++;
    }
    if (!ret.isLast()) {
      System.out.println("next");
    }
    cli.setLastCommand(cli.getCommand());
  }

}
