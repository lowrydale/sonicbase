package com.sonicbase.database.performance;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class TestPerformance {

  public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, ExecutionException, IOException {
    com.sonicbase.bench.TestPerformance test = new com.sonicbase.bench.TestPerformance();
    test.run(args);
  }
}
