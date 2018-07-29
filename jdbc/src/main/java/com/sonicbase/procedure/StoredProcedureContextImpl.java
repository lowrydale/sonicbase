package com.sonicbase.procedure;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.jdbcdriver.ConnectionProxy;


@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class StoredProcedureContextImpl implements StoredProcedureContext {
  private int shard;
  private int replica;
  private ObjectNode config;
  private long storedProdecureId;
  private SonicBaseConnectionImpl connection;
  private Parameters parameters;
  private RecordEvaluator recordEvaluator;
  private int viewVersion;

  @Override
  public Parameters getParameters() {
    return parameters;
  }

  @Override
  public int getViewVersion() {
    return viewVersion;
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
    return new StoredProcedureResponseImpl(((ConnectionProxy)connection.getConnection()).getDatabaseClient().getCommon());
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

  public void setViewVersion(int viewVersion) {
    this.viewVersion = viewVersion;
  }
}
