package com.sonicbase.procedure;

import java.util.List;

public interface StoredProcedure {

  void init(StoredProcedureContext context);

  StoredProcedureResponse execute(StoredProcedureContext context);

  StoredProcedureResponse finalize(StoredProcedureContext context, List<StoredProcedureResponse> responses);

}
