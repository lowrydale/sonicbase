package com.sonicbase.procedure;

import java.sql.*;

public class StoredProcedureClient {

  public static void main(String[] args) throws SQLException, ClassNotFoundException {
    Class.forName("com.sonicbase.jdbcdriver.Driver");
    Connection conn = DriverManager.getConnection("jdbc:sonicbase:127.0.0.1:9010/db", "user", "password");

    if (args[0].equals("1")) {
      String query = "call procedure 'com.sonicbase.procedure.MyStoredProcedure1'";
      try (PreparedStatement procedureStmt = conn.prepareStatement(query);
          ResultSet rs = procedureStmt.executeQuery()) {
        while (rs.next()) {
          System.out.println("id=" + rs.getLong("id1") + ", socialsecuritynumber=" +
              rs.getString("socialsecuritynumber") + ", gender=" + rs.getString("gender"));
        }
        System.out.println("Finished");
      }
    }
    else if (args[0].equals("2")) {
      String tableName = null;
      try {
        String query = "call procedure 'com.sonicbase.procedure.MyStoredProcedure2', 1000";
        try (PreparedStatement procedureStmt = conn.prepareStatement(query);
             ResultSet procedureRs = procedureStmt.executeQuery()) {
          if (procedureRs.next()) {
            tableName = procedureRs.getString("tableName");
          }
          System.out.println("tableName=" + tableName);
        }

        try (PreparedStatement resultsStmt = conn.prepareStatement("select * from " + tableName);
            ResultSet rs = resultsStmt.executeQuery()) {
          int offset = 3;
          while (rs.next()) {
            System.out.println("id=" + rs.getLong("id1") + ", socialsecuritynumber=" +
                rs.getString("socialsecuritynumber") + ", gender=" + rs.getString("gender"));
          }
        }
        System.out.println("finished");
      }
      finally {
        try {
          if (tableName != null) {
            try (PreparedStatement stmt = conn.prepareStatement("drop table " + tableName)) {
              stmt.executeUpdate();
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    else if (args[0].equals("3")) {
      String query = "call procedure 'com.sonicbase.procedure.MyStoredProcedure3'";
      try (PreparedStatement procedureStmt = conn.prepareStatement(query);
          ResultSet rs = procedureStmt.executeQuery()) {
        int offset = 3;
        while (true) {
          if (!rs.next()) {
            break;
          }
          System.out.println("id=" + rs.getLong("id1") + ", socialsecuritynumber=" +
              rs.getString("socialsecuritynumber") + ", gender=" + rs.getString("gender"));
        }
        System.out.println("Finished");
      }
    }
    else if (args[0].equals("4")) {
      String query = "call procedure 'com.sonicbase.procedure.MyStoredProcedure4'";
      try (PreparedStatement procedureStmt = conn.prepareStatement(query);
           ResultSet rs = procedureStmt.executeQuery()) {
        while (true) {
          if (!rs.next()) {
            break;
          }
          System.out.println("id=" + rs.getLong("id1") + ", socialsecuritynumber=" +
              rs.getString("socialsecuritynumber") + ", gender=" + rs.getString("gender"));
        }
        System.out.println("Finished");
      }
    }
  }
}
