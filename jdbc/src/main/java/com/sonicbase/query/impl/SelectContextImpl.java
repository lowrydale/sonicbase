package com.sonicbase.query.impl;

import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class SelectContextImpl {
  private boolean restrictToThisServer;
  private boolean currPartitions;
  private int lastShard;
  private SelectStatementImpl selectStatement;
  private int nextShard = -1;
  private Object[] nextKey;
  private Object[][][] currKeys;
  private Object[][][] lastKeys;
  private ExpressionImpl.RecordCache recordCache;
  private String[] tableNames;
  private String indexName;
  private BinaryExpression.Operator operator;
  private Boolean sortWithIndex;
  private StoredProcedureContextImpl procedureContext;

  public SelectContextImpl(
      String tableName, String indexName, BinaryExpression.Operator operator,
      int nextShard, Object[] nextKey,
      Object[][][] keys, ExpressionImpl.RecordCache recordCache, int lastShard, boolean currPartitions)  {
    this.tableNames = new String[]{tableName};
    this.indexName = indexName;
    this.operator = operator;
    this.nextShard = nextShard;
    this.nextKey = nextKey;
    this.recordCache = recordCache;
    this.currKeys = keys;
    this.lastShard = lastShard;
    this.currPartitions = currPartitions;
  }

  public SelectContextImpl(ExpressionImpl.NextReturn tableIds, boolean canUseIndex, String[] tableNames,
                           int nextShard, Object[] nextKey,
                           SelectStatementImpl selectStatement, ExpressionImpl.RecordCache recordCache,
                           boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    if (tableIds != null) {
      this.currKeys = tableIds.getKeys();
      this.recordCache = recordCache;
    }
    this.restrictToThisServer = restrictToThisServer;
    this.procedureContext = procedureContext;
    this.sortWithIndex = canUseIndex;
    this.tableNames = tableNames;
    this.selectStatement = selectStatement;
    this.nextShard = nextShard;
    this.nextKey = nextKey;
  }

  public SelectContextImpl() {

  }

  public boolean isCurrPartitions() {
    return currPartitions;
  }

  public int getLastShard() {
    return lastShard;
  }

  public Boolean getSortWithIndex() {
    return sortWithIndex;
  }

  public String[] getTableNames() {
    return tableNames;
  }

  public String getIndexName() {
    return indexName;
  }

  public BinaryExpression.Operator getOperator() {
    return operator;
  }

  public int getNextShard() {
    return nextShard;
  }

  public Object[] getNextKey() {
    return nextKey;
  }

  public Object[][][] getCurrKeys() {
    return currKeys;
  }

  public Object[][][] getLastKeys() {
    return lastKeys;
  }

  public void setNextKey(Object[] nextKey) {
    this.nextKey = nextKey;
  }

  public void setNextShard(int nextShard) {
    this.nextShard = nextShard;
  }

  public void setOperator(BinaryExpression.Operator operator) {
    this.operator = operator;
  }

  public void setCurrKeys(Object[][][] ids) {
    this.lastKeys = this.currKeys;
    this.currKeys = ids;
  }

  public void setSortWithIndex(Boolean sortWithIndex) {
    this.sortWithIndex = sortWithIndex;
  }

  public SelectStatementImpl getSelectStatement() {
    return selectStatement;
  }

  public ExpressionImpl.RecordCache getRecordCache() {
    return recordCache;
  }

  public void setRecordCache(ExpressionImpl.RecordCache recordCache) {
    this.recordCache = recordCache;
  }

  public boolean isRestrictToThisServer() {
    return restrictToThisServer;
  }

  public StoredProcedureContextImpl getProcedureContext() {
    return procedureContext;
  }
}
