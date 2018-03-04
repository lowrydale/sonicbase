package com.sonicbase.procedure;

import com.sonicbase.query.DatabaseException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class MyStoredProcedure implements StoredProcedure {

  public MyStoredProcedure() {

    String query = "execute procedure 'com.sonicbase.procedure.MyStoredProcedure', 'select * from persons where id>0'";
  }
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
  public StoredProcedureResponse execute(StoredProcedureContext context) {
    try {
      String query = context.getParameters().getString(2);

      SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(context, query);
      stmt.restrictToThisServer(true);

      ResultSet rs = stmt.executeQueryWithEvaluator(new RecordEvaluator(){
        @Override
        public boolean evaluate(final StoredProcedureContext context, Record record) {
          if (!record.getDatabase().equalsIgnoreCase("db") ||
              !record.getTableName().equalsIgnoreCase("persons")) {
            return false;
          }
          Long id = record.getLong("id");
          if (id != null && id > 50 && id < 100) {
            return true;
          }
          return false;
        }
      });
      while(rs.next()) {

        if (computeResult(rs)) {
          PreparedStatement insertStmt = context.getConnection().prepareStatement("insert into " +
              context.getStoredProdecureId() + "_results (id, socialsecuritynumber, gender) VALUES (?, ?, ?)");
          insertStmt.setLong(1, rs.getLong("id"));
          insertStmt.setString(2, rs.getString("socialsecuritynumber"));
          insertStmt.setString(3, rs.getString("gender"));
          insertStmt.executeUpdate();
          insertStmt.close();
        }
      }
      stmt.close();

    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return null;
  }

  public StoredProcedureResponse finalize(StoredProcedureContext context,
                                          List<StoredProcedureResponse> responses) {
    StoredProcedureResponse response = context.createResponse();
    Record record = context.createRecord();
    record.setString("tableName", context.getStoredProdecureId() + "_results");
    response.addRecord(record);
    return response;
  }

  private boolean computeResult(ResultSet rs) {
    return true;
  }
}
