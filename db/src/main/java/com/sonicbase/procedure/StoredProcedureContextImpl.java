package com.sonicbase.procedure;

import com.fasterxml.jackson.databind.node.ObjectNode;


public class StoredProcedureContextImpl implements StoredProcedureContext {
  private int shard;
  private int replica;
  private ObjectNode config;
  private long storedProdecureId;
  private SonicBaseConnectionImpl connection;
  private Parameters parameters;
  private RecordEvaluator recordEvaluator;

  @Override
  public Parameters getParameters() {
    return parameters;
  }

  public int getShard() {
    return shard;
  }

  public int getReplica() {
    return replica;
  }

  public ObjectNode getConfig() {
    return config;
  }

  public long getStoredProdecureId() {
    return storedProdecureId;
  }

  public SonicBaseConnectionImpl getConnection() {
    return connection;
  }

  @Override
  public StoredProcedureResponse createResponse() {
    return new StoredProcedureResponseImpl();
  }

  @Override
  public Record createRecord() {
    return new RecordImpl();
  }

  public void setShard(int shard) {
    this.shard = shard;
  }

  public void setReplica(int replica) {
    this.replica = replica;
  }

  public void setConfig(ObjectNode config) {
    this.config = config;
  }

  public void setStoredProdecureId(long storedProdecureId) {
    this.storedProdecureId = storedProdecureId;
  }

  public void setConnection(SonicBaseConnectionImpl connection) {
    this.connection = connection;
  }

  public void setParameters(Parameters parameters) {
    this.parameters = parameters;
  }

  public void setRecordEvaluator(RecordEvaluator recordEvaluator) {
    this.recordEvaluator = recordEvaluator;
  }

  public RecordEvaluator getRecordEvaluator() {
    return recordEvaluator;
  }
}
