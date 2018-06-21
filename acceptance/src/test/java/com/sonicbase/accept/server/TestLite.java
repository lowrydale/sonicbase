package com.sonicbase.accept.server;

import com.sonicbase.client.DatabaseClient;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.Offset;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestLite {

  @Test
  public void testLimitOffset() {

    Limit limit = new Limit();
    limit.setRowCount(50);
    Offset offset = new Offset();
    offset.setOffset(1000);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons limit 50 offset 1000", limit, offset);
    System.out.println(removed);
    assertEquals(removed, "select * from persons limit x offset x");
  }

  @Test
  public void testLimitOffset2() {

    Offset offset = new Offset();
    offset.setOffset(10);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons offset 10", null, offset);
    System.out.println(removed);
    assertEquals(removed, "select * from persons offset x");
  }

  @Test
  public void testLimitOffset3() {

    Limit limit = new Limit();
    limit.setRowCount(50);
    String removed = DatabaseClient.removeOffsetAndLimit("select * from persons limit 50", limit, null);
    System.out.println(removed);
    assertEquals(removed, "select * from persons limit x");
  }

}
