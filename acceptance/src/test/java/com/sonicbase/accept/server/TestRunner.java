package com.sonicbase.accept.server;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;
import org.testng.TestNG;

import java.util.List;

public class TestRunner {

  public static void main(String[] args) throws InterruptedException {

    runTests(new Class[]{
        TestBulkImport.class,
        TestDatabase.class,
        TestDatabaseAdvanced.class,
        TestDataTypes.class,
        TestDataUtils.class,
        TestDateUtils.class,
        TestFunctions.class,
        TestIndex.class,
        TestInsertSelect.class,
        TestJoins.class,
        TestLite.class,
        TestLogManager.class,
        TestLongManagerLostEntries.class,
        TestLongRunningCommands.class,
        TestSchema.class,
        TestSecondaryIndex.class,
        TestSetOperations.class,
        TestSnapshotManager.class,
        TestSnapshotManagerLostEntries.class,
        TestStoredProcedures.class,
        TestTransactions.class});
  }

  private static void runTests(Class[] classes) throws InterruptedException {
    for (Class clz : classes) {
      runTest(clz);
    }
  }

  private static void runTest(Class clz) throws InterruptedException {
    try {
      TestListenerAdapter tla = new TestListenerAdapter();
      TestNG testNG = new TestNG();
      testNG.setTestClasses(new Class[]{clz});
      testNG.addListener(tla);
      testNG.run();

      List<ITestResult> failedTests = Lists.newArrayList();
      failedTests.addAll(tla.getFailedTests());
      failedTests.addAll(tla.getConfigurationFailures());
      if (!failedTests.isEmpty()) {
        String header = String.format("Combined Messages (Total:%d)", failedTests.size());

        List<String> errorMessages = Lists.newArrayList();
        errorMessages.add(header);

        StringBuilder builder = new StringBuilder();
        int i = 0;
        for (ITestResult testResult : failedTests) {
          String stackTraceString = Throwables.getStackTraceAsString(testResult.getThrowable());
          String template = "Message-%d: %n %s";
          builder.append(String.format(template, i++, stackTraceString)).append("\n");

        }
        System.out.println("Test errors: class=" + clz.getName() + ", errors=" + builder.toString());
      }
      System.out.println("Finished Test: class=" + clz.getName());
    }
    catch (Exception e) {
      System.out.println("Error running test: class=" + clz.getName());
      e.printStackTrace();
    }
    Thread.sleep(10_000);
  }
}
