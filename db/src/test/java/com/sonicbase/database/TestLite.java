package com.sonicbase.database;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.util.ISO8601;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Date;

/**
 * Created by lowryda on 6/17/17.
 */
public class TestLite {

  @Test
  public void test() {
    System.out.println(ISO8601.to8601String(new Date(System.currentTimeMillis())));
  }

  @Test
  public void testSchema() throws InterruptedException, IOException {

    File schemaFile = new File(System.getProperty("user.home"), "tmp/schema.bin");
    try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(schemaFile)))) {
      DatabaseCommon common = new DatabaseCommon();
      common.deserializeSchema(in);

      Thread.sleep(200000);
    }

  }
}
