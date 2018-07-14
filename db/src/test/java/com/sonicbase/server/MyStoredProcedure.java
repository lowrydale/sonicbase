/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.procedure.Record;
import com.sonicbase.procedure.StoredProcedure;
import com.sonicbase.procedure.StoredProcedureContext;
import com.sonicbase.procedure.StoredProcedureResponse;

import java.util.List;

public class MyStoredProcedure implements StoredProcedure {

  public static boolean called = false;

  public void init(StoredProcedureContext context) {
  }

  @Override
  public StoredProcedureResponse execute(final StoredProcedureContext context) {
    called = true;
    return null;
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