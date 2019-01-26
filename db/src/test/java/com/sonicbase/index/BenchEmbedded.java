/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.embedded.EmbeddedDatabase;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BenchEmbedded {
  //public static void main(String[] args) throws SQLException {
  @Test(enabled=false)
  public void test() throws SQLException {
    EmbeddedDatabase embedded = new EmbeddedDatabase();

    embedded.enableDurability(System.getProperty("user.home") + "/db-data.embedded");
    embedded.setUseUnsafe(false);
    embedded.purge();
    embedded.start();
    embedded.createDatabaseIfNeeded("test");
    Connection embeddedConn = embedded.getConnection("test");
    DatabaseClient embeddedClient = ((ConnectionProxy)embeddedConn).getDatabaseClient();

    PreparedStatement stmt = embeddedConn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, PRIMARY KEY (id))");
    stmt.executeUpdate();
    stmt.close();

    for (int i = 0; i < 1000; i++) {
      stmt = embeddedConn.prepareStatement("insert into persons (id, id2) values(" + i + ", " + i + ")");
      stmt.executeUpdate();
      stmt.close();
    }
    System.out.println("finished load");

    for (int i = 0; i < 50; i++) {
      long begin = System.currentTimeMillis();
      stmt = embeddedConn.prepareStatement("select * from persons where id >= 0");
      ResultSet rs = stmt.executeQuery();
      while (rs.next()) {
        rs.getLong("id");
      }
      System.out.println("finished range step=" + i + ", duration=" + (System.currentTimeMillis() - begin));
    }
    System.out.println("finished all range");
    stmt.close();

    embeddedConn.close();

    embedded.shutdown();
  }
}
