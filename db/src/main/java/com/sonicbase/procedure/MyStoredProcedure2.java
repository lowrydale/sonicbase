package com.sonicbase.procedure;

import com.sonicbase.query.DatabaseException;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

public class MyStoredProcedure2 implements StoredProcedure {

  public void init(StoredProcedureContext context) {
    try {
      SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(
          context, "create table " + context.getStoredProdecureId() + "_results (id BIGINT, num DOUBLE, socialSecurityNumber " +
              "VARCHAR(20), gender VARCHAR(8), PRIMARY KEY (id))");
      stmt.executeUpdate();
      stmt.close();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  @Override
  public StoredProcedureResponse execute(final StoredProcedureContext context) {
    try {
      String query = context.getParameters().getString(2);

      SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(context, query);
      stmt.restrictToThisServer(true);

      final List<Record> batch = new ArrayList<>();
      stmt.executeQueryWithEvaluator(new RecordEvaluator() {
        @Override
        public boolean evaluate(final StoredProcedureContext context, Record record) {
          if (record.getDatabase().equalsIgnoreCase("db") &&
              record.getTableName().equalsIgnoreCase("persons")) {
            Long id = record.getLong("id");
            if (id != null && id > 50 && id < context.getParameters().getInt(3) && passesComplicatedLogic(record)) {
              batch.add(record);
              if (batch.size() >= 200) {
                insertBatch(context, batch);
                batch.clear();
              }
            }
          }
          return false;
        }
      });

      if (batch.size() != 0) {
        insertBatch(context, batch);
      }

      stmt.close();
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

  public void insertBatch(StoredProcedureContext context, List<Record> batch) {
    try {
      PreparedStatement insertStmt = context.getConnection().prepareStatement("insert into " +
          context.getStoredProdecureId() + "_results (id, socialsecuritynumber, gender) VALUES (?, ?, ?)");
      for (Record record : batch) {
        insertStmt.setLong(1, record.getLong("id"));
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
    record.setString("tableName",context.getStoredProdecureId() + "_results");

    StoredProcedureResponse response = context.createResponse();
    response.addRecord(record);

    return response;
  }
}
