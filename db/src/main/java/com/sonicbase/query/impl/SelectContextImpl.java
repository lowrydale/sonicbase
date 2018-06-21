package com.sonicbase.query.impl;

import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;

import java.io.IOException;

/**
 * Responsible for
 */
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public SelectContextImpl(
      String tableName, String indexName, BinaryExpression.Operator operator,
      int nextShard, Object[] nextKey,
      Object[][][] keys, ExpressionImpl.RecordCache recordCache, int lastShard, boolean currPartitions) throws IOException {
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public Object[] getNextKey() {
    return nextKey;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP", justification="copying the returned data is too slow")
  public Object[][][] getCurrKeys() {
    return currKeys;
  }

  public Object[][][] getLastKeys() {
    return lastKeys;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public void setNextKey(Object[] nextKey) {
    this.nextKey = nextKey;
  }

  public void setNextShard(int nextShard) {
    this.nextShard = nextShard;
  }

  public void setOperator(BinaryExpression.Operator operator) {
    this.operator = operator;
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EI_EXPOSE_REP2", justification="copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
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
