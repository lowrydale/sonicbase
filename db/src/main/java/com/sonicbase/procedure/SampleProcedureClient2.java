/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.procedure;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SampleProcedureClient2 {

  public static void main(String[] args) throws PropertyVetoException, ClassNotFoundException, SQLException {
    Class.forName("com.sonicbase.jdbcdriver.Driver");

    final ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setDriverClass("Driver"); //loads the jdbc driver
    cpds.setJdbcUrl("jdbc:sonicbase:localhost:9010/db");

    cpds.setMinPoolSize(5);
    cpds.setAcquireIncrement(1);
    cpds.setMaxPoolSize(20);
    String tableName = null;
    Connection connection = cpds.getConnection();
    try {
      String query = "call com.sonicbase.procedure.MyStoredProcedure2('select * from persons where id>50 and gender=\'m\'')";
      PreparedStatement procedureStmt = connection.prepareStatement(query);
      ResultSet procedureRs = procedureStmt.executeQuery();
      if (procedureRs.next()) {
        tableName = procedureRs.getString("tableName");
        PreparedStatement resultsStmt = connection.prepareStatement("select * from " + tableName);
        ResultSet rsultsRs = resultsStmt.executeQuery();
        while (rsultsRs.next()) {
          System.out.println(
              "id=" + rsultsRs.getLong("id") +
                  ", socialsecuritynumber=" + rsultsRs.getString("socialsecuritynumber") +
                  ", gender=" + rsultsRs.getString("gender"));
        }
        rsultsRs.close();
        resultsStmt.close();

      }
      procedureRs.close();
      procedureStmt.close();
    }
    finally {
      try {
        if (tableName != null) {
          PreparedStatement stmt = connection.prepareStatement("drop table " + tableName);
          stmt.executeUpdate();
          stmt.close();
        }
      }
      catch (Exception e) {
        e.printStackTrace();
      }

      connection.close();
      cpds.close();
    }
  }
}
