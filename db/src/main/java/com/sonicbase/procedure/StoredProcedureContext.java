package com.sonicbase.procedure;

import com.fasterxml.jackson.databind.node.ObjectNode;


public interface StoredProcedureContext {

  int getShard();

  int getReplica();

  ObjectNode getConfig();

  long getStoredProdecureId();

  SonicBaseConnection getConnection();

  StoredProcedureResponse createResponse();

  Record createRecord();

  Parameters getParameters();
}
