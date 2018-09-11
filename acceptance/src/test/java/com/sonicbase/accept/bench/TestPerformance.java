/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.bench;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class TestPerformance {
  public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException, ExecutionException, IOException {
    com.sonicbase.bench.TestPerformance perf = new com.sonicbase.bench.TestPerformance();
    perf.run(args);
  }
}
