package com.sonicbase.procedure;

import com.sonicbase.query.DatabaseException;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class MyStoredProcedure4 implements StoredProcedure {

  public void init(StoredProcedureContext context) {
  }

  @Override
  public StoredProcedureResponse execute(StoredProcedureContext context) {
    try {
      String query = "select * from persons where id1 > 1";

      SonicBasePreparedStatement stmt = context.getConnection().prepareSonicBaseStatement(context, query);
      stmt.restrictToThisServer(true);

      final StoredProcedureResponse response = context.createResponse();

      ResultSet rs = stmt.executeQueryWithEvaluator(new RecordEvaluator(){
        @Override
        public boolean evaluate(final StoredProcedureContext context, Record record) {
          if (!record.getDatabase().equalsIgnoreCase("db") ||
              !record.getTableName().equalsIgnoreCase("persons")) {
            return false;
          }
          Long id = record.getLong("id1");
          if (id != null && id > 2 && id < 1000 && passesComplicatedLogic(record)) {
            if (!record.isDeleting()) {
              response.addRecord(record);
            }
          }
          return false;
        }
      });

      stmt.close();
      return response;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public StoredProcedureResponse finalize(StoredProcedureContext context,
                                          List<StoredProcedureResponse> responses) {
    List<Record> records = new ArrayList();
    for (StoredProcedureResponse currResponse : responses) {
      records.addAll(currResponse.getRecords());
    }

    Collections.sort(records, new Comparator<Record>() {
      @Override
      public int compare(Record o1, Record o2) {
        return Long.compare(o1.getLong("id1"), o2.getLong("id1"));
      }
    });

    StoredProcedureResponse response = context.createResponse();
    response.setRecords(records);
    return response;
  }

  private boolean passesComplicatedLogic(Record rs) {
    //put complicated logic here
    return true;
  }
}
