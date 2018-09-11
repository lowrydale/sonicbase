package com.sonicbase.procedure;

import com.sonicbase.query.DatabaseException;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class MyStoredProcedure2 implements StoredProcedure {

  public void init(StoredProcedureContext context) {
    try {
      try (SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(
          context, "create table " + "results_" + context.getStoredProdecureId() + " (id1 BIGINT, num DOUBLE, socialSecurityNumber " +
              "VARCHAR(20), gender VARCHAR(8), PRIMARY KEY (id1))")) {
        stmt.executeUpdate();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public StoredProcedureResponse execute(final StoredProcedureContext context) {
    try {
      String query = "select * from persons where id1>1 and id1<500 and gender='m'";

      try (SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(context, query)) {
        stmt.restrictToThisServer(true);

        final List<Record> batch = new ArrayList<>();
        stmt.executeQueryWithEvaluator(new RecordEvaluator() {
          @Override
          public boolean evaluate(final StoredProcedureContext context, Record record) {
            if (record.getDatabase().equalsIgnoreCase("db") &&
                record.getTableName().equalsIgnoreCase("persons")) {
              Long id = record.getLong("id1");
              if (id != null && id > 2 && id < context.getParameters().getInt(2) && passesComplicatedLogic(record)) {
                if (!record.isDeleting()) {
                  batch.add(record);
                  if (batch.size() >= 200) {
                    insertBatch(context, batch);
                    batch.clear();
                  }
                }
              }
            }
            return false;
          }
        });

        if (batch.size() != 0) {
          insertBatch(context, batch);
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  private boolean passesComplicatedLogic(Record record) {
    //put complicated logic here
    return true;
  }

  private void insertBatch(StoredProcedureContext context, List<Record> batch) {
    try {
      PreparedStatement insertStmt = context.getConnection().prepareStatement("insert into " +
           "results_" + context.getStoredProdecureId() + " (id1, socialsecuritynumber, gender) VALUES (?, ?, ?)");
      for (Record record : batch) {
        insertStmt.setLong(1, record.getLong("id1"));
        insertStmt.setString(2, record.getString("socialsecuritynumber"));
        insertStmt.setString(3, record.getString("gender"));
        insertStmt.addBatch();
      }
      insertStmt.executeBatch();
      insertStmt.close();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public StoredProcedureResponse finalize(StoredProcedureContext context,
                                          List<StoredProcedureResponse> responses) {
    Record record = context.createRecord();
    record.setString("tableName", "results_" + context.getStoredProdecureId());

    StoredProcedureResponse response = context.createResponse();
    response.addRecord(record);

    return response;
  }
}
