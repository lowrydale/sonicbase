/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.cli;

import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SQLHandler {
  public static final String ERROR_NOT_USING_A_DATABASE_STR = "Error, not using a database";
  private final Cli cli;

  SQLHandler(Cli cli) {
    this.cli = cli;
  }

  void select(String command) throws SQLException, JSQLParserException, ClassNotFoundException, IOException,
      InterruptedException {

    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing select request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    cli.setRet(stmt.executeQuery());

    cli.setLastCommand(command);
    cli.processResults(command, cli.getConn());

  }

  public void insert(String command) throws SQLException, ClassNotFoundException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing insert request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished insert: count=" + count);
  }

  public void delete(String command) throws SQLException, ClassNotFoundException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing delete request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished delete: count=" + count);
  }

  void truncate(String command) throws SQLException, ClassNotFoundException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing truncate request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished truncate: count=" + count);
  }

  void drop(String command) throws SQLException, ClassNotFoundException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing drop request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished drop: count=" + count);
  }

  void create(String command) throws SQLException, ClassNotFoundException {
    cli.initConnection();

    if (command.startsWith("create database")) {
      String dbName = command.split(" ")[2].trim();
      cli.getConn().createDatabase(dbName);
      cli.useDatabase(dbName);
      return;
    }

    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.println("Executing create request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished create: count=" + count);
  }

  void alter(String command) throws SQLException, ClassNotFoundException {
    if (cli.getCurrDbName() == null) {
      cli.println(ERROR_NOT_USING_A_DATABASE_STR);
      return;
    }

    cli.initConnection();

    cli.println("Executing alter request");

    PreparedStatement stmt = cli.getConn().prepareStatement(command);
    int count = stmt.executeUpdate();
    cli.println("Finished alter: count=" + count);
  }

}
