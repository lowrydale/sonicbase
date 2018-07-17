package com.sonicbase.procedure;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public interface SonicBasePreparedStatement extends PreparedStatement {

  void restrictToThisServer(boolean restrict);

  boolean isRestrictedToThisServer();

  ResultSet executeQueryWithEvaluator(RecordEvaluator evaluator) throws SQLException;

  ResultSet executeQueryWithEvaluator(String evaluatorClassName) throws SQLException;
}
