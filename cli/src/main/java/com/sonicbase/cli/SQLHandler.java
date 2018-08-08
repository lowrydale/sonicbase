/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLHandler {

  private final Cli cli;

  public SQLHandler(Cli cli) {
    this.cli = cli;
  }

  public void select(String command) throws SQLException, JSQLParserException, ClassNotFoundException, IOException,
      InterruptedException {
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

    System.out.println("Executing select request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    cli.setRet(stmt.executeQuery());

    cli.setLastCommand(command);
    cli.processResults(command, cli.getConn());

  }

  public void insert(String command) throws SQLException, ClassNotFoundException {
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

    System.out.println("Executing insert request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished insert: count=" + count);
  }

  public void delete(String command) throws SQLException, ClassNotFoundException {
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

    System.out.println("Executing delete request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished delete: count=" + count);
  }

  public void truncate(String command) throws SQLException, ClassNotFoundException {
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

    System.out.println("Executing truncate request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished truncate: count=" + count);
  }

  public void drop(String command) throws SQLException, ClassNotFoundException {
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

    System.out.println("Executing drop request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished drop: count=" + count);
  }

  public void create(String command) throws SQLException, ClassNotFoundException {
    String cluster = cli.getCurrCluster();
    if (cluster == null) {
      System.out.println("Error, not using a cluster");
      return;
    }

    cli.initConnection();

    if (command.startsWith("create database")) {
      String dbName = command.split(" ")[2].trim();
      cli.getConn().createDatabase(dbName);
      cli.useDatabase(dbName);
      return;
    }

    if (cli.getCurrDbName() == null) {
      System.out.println("Error, not using a database");
      return;
    }

    System.out.println("Executing create request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished create: count=" + count);
  }

  public void alter(String command) throws SQLException, ClassNotFoundException {
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

    System.out.println("Executing alter request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    System.out.println("Finished alter: count=" + count);
  }

}
