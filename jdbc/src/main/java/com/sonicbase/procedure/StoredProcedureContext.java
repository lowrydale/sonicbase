package com.sonicbase.procedure;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.Config;


public interface StoredProcedureContext {

  /**
   * @return the shard this stored procedure is running on
   */
  int getShard();

  /**
   * @return the replica this stored procedure is running on
   */
  int getReplica();

  /**
   * @return the configuration object for this cluster
   */
  Config getConfig();

  /**
   * @return a number that uniquely identifies this stored procedure
   */
  long getStoredProdecureId();

  /**
   * @return a SonicBaseConnection
   */
  SonicBaseConnection getConnection();

  /**
   * @return a new StoredProcedureResponse
   */
  StoredProcedureResponse createResponse();

  /**
   * @return a new Record
   */
  Record createRecord();

  /**
   * @return parameters passed to the stored procedure from the client
   */
  Parameters getParameters();

  /**
   * @return current view version of the data
   */
  int getViewVersion();
}
