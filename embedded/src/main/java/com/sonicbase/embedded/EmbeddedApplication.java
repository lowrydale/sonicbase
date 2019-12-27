/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.embedded;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class EmbeddedApplication {

  public static void main(String[] args) throws SQLException {
    EmbeddedDatabase db = new EmbeddedDatabase();
    try {
      db.setUseUnsafe(false);
      db.enableDurability(System.getProperty("user.home") + "/db-data.embedded");
      db.purge();
      db.start();

      db.createDatabaseIfNeeded("db");

      try (Connection conn = db.getConnection("db")) {
        try (PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, socialSecurityNumber VARCHAR(20), gender VARCHAR(8), PRIMARY KEY (id))")) {
          stmt.executeUpdate();
        }

        for (int i = 0; i < 10; i++) {
          try (PreparedStatement stmt = conn.prepareStatement("insert into persons (id, socialSecurityNumber, gender) VALUES (?, ?, ?)")) {
            stmt.setLong(1, i);
            stmt.setString(2, "933-28-" + i);
            stmt.setString(3, "m");
            stmt.executeUpdate();
          }
        }

        try (PreparedStatement stmt = conn.prepareStatement("select * from persons where id < 2")) {
          try (ResultSet rs = stmt.executeQuery()) {
            rs.next();
            System.out.println("personId=" + rs.getLong("id"));
            rs.next();
            System.out.println("personId=" + rs.getLong("id"));
          }
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      db.shutdown();
    }

  }
}
