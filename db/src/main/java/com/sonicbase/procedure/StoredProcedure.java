package com.sonicbase.procedure;

import java.util.List;

public interface StoredProcedure {

  /**
   * Called once an a random server.
   * @param context StoredProcedureContext
   */
  void init(StoredProcedureContext context);

  /**
   * Called once on a replica of every shard.
   * @param context StoredProcedureContext
   * @return StoredProcedureResponse including results that should be returned the the client
   */
  StoredProcedureResponse execute(StoredProcedureContext context);

  /**
   * Called once on the server where init was called.
   * @param context StoredProcedureContext
   * @param responses list of StoredProcedureResponses that were returned from each of the execute calls
   * @return StoredProcedureResponse including results that should be returned the the client
   */
  StoredProcedureResponse finalize(StoredProcedureContext context, List<StoredProcedureResponse> responses);

}
