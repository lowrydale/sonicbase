package com.sonicbase.server;

import com.sonicbase.common.Record;
import com.sonicbase.common.RecordLockedException;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class TransactionManager {

  public enum OperationType {
    BATCH_INSERT_WITH_RECORD,
    INSERT_WITH_RECORD,
    BATCH_INSERT,
    INSERT,
    UPDATE,
    DELETE_RECORD,
    DELETE_INDEX_ENTRY,
    DELETE_ENTRY_BY_KEY
  }

  private final com.sonicbase.server.DatabaseServer server;
  private final ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Object[], RecordLock>>> locks =
      new ConcurrentHashMap<>();

  TransactionManager(
      DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public Map<Long, Transaction> getTransactions() {
    return transactions;
  }

  Map<String, ConcurrentSkipListMap<Object[], RecordLock>> getLocks(String dbName) {
    return locks.get(dbName);
  }

  Transaction getTransaction(long transactionId) {
    return transactions.get(transactionId);
  }

  public static class RecordLock {
    private Transaction transaction;
    private String tableName;
    private String indexName;
    private Object[] primaryKey;
    private int lockCount;

    public String getTableName() {
      return tableName;
    }

    public String getIndexName() {
      return indexName;
    }

    public Object[] getPrimaryKey() {
      return primaryKey;
    }
  }

  static class Operation {
    private final OperationType type;
    private final byte[] body;
    private final String command;
    private final boolean replayed;

    Operation(OperationType type, String command, byte[] body, boolean replayedCommand) {
      this.type = type;
      this.body = body;
      this.command = command;
      this.replayed = replayedCommand;
    }

    public OperationType getType() {
      return type;
    }

    public String getCommand() {
      return command;
    }

    public byte[] getBody() {
      return body;
    }

    public boolean getReplayed() {
      return replayed;
    }
  }

  static class Transaction {
    private final long id;
    private final List<RecordLock> locks = new ArrayList<>();
    private final ConcurrentHashMap<String, List<Record>> records = new ConcurrentHashMap<>();
    private final List<Operation> operations = new ArrayList<>();

    Transaction(long transactionId) {
      this.id = transactionId;
    }

    public Map<String, List<Record>> getRecords() {
      return records;
    }

    void addOperation(OperationType type, String command, byte[] body, boolean replayedCommand) {
      operations.add(new Operation(type, command, body, replayedCommand));
    }

    List<Operation> getOperations() {
      return operations;
    }

    List<RecordLock> getLocks() {
      return locks;
    }
  }

  void deleteLock(String dbName, String tableName, long transactionId, TableSchema tableSchema, Object[] primaryKey) {
    synchronized (locks) {
      ConcurrentSkipListMap<Object[], RecordLock> tableLocks = locks.get(dbName).get(tableName);
      RecordLock lock = tableLocks.get(primaryKey);
      lock.lockCount -= 1;
      if (lock.lockCount == 0) {
        tableLocks.remove(primaryKey);
        Transaction trans = transactions.get(transactionId);
        for (int i = 0; i < trans.locks.size(); i++) {
          lock = trans.locks.get(i);
          if (deleteLockForTable(dbName, tableName, transactionId, tableSchema, primaryKey, lock, trans, i)) {
            break;
          }
        }
      }
    }
  }

  private boolean deleteLockForTable(String dbName, String tableName, long transactionId, TableSchema tableSchema,
                                     Object[] primaryKey, RecordLock lock, Transaction trans, int i) {
    if (lock.tableName.equals(tableName)) {
      Comparator[] comparators = null;
      for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
        if (entry.getValue().isPrimaryKey()) {
          comparators = entry.getValue().getComparators();
        }
      }
      if (comparators == null) {
        throw new DatabaseException("Comparators are null: dbName=" + dbName + ", table=" + tableName);
      }
      return doDeleteLock(transactionId, primaryKey, lock, trans, i, comparators);
    }
    return false;
  }

  private boolean doDeleteLock(long transactionId, Object[] primaryKey, RecordLock lock, Transaction trans, int i,
                               Comparator[] comparators) {
    boolean mismatch = false;
    for (int j = 0; j < primaryKey.length; j++) {
      if (comparators[j].compare(primaryKey[j], lock.primaryKey[j]) != 0) {
        mismatch = true;
        break;
      }
    }
    if (!mismatch) {
      trans.locks.remove(i);
      if (trans.locks.isEmpty()) {
        transactions.remove(transactionId);
      }
      return true;
    }
    return false;
  }

  void preHandleTransaction(String dbName, String tableName, String indexName, boolean isExplicitTrans,
                            boolean isCommitting, long transactionId, Object[] primaryKey,
                            AtomicBoolean shouldExecute, AtomicBoolean shouldDeleteLock) {
    ConcurrentSkipListMap<Object[], RecordLock> tableLocks;
    if (!locks.containsKey(dbName)) {
      locks.put(dbName, new ConcurrentHashMap<>());
    }
    tableLocks = locks.get(dbName).get(tableName);

    tableLocks = createTableLocks(dbName, tableName, tableLocks);

    doPreHandleTransaction(tableName, indexName, isExplicitTrans, isCommitting, transactionId, primaryKey,
        shouldExecute, shouldDeleteLock, tableLocks);
  }

  private ConcurrentSkipListMap<Object[], RecordLock> createTableLocks(
      String dbName, String tableName, ConcurrentSkipListMap<Object[], RecordLock> tableLocks) {
    synchronized (locks) {
      if (tableLocks == null) {
        IndexSchema primaryKeySchema = null;
        for (Map.Entry<String, IndexSchema> entry :
            server.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            primaryKeySchema = entry.getValue();
            break;
          }
        }
        if (primaryKeySchema == null) {
          throw new DatabaseException("primaryKeySchema is null: dbName=" + dbName + ", table=" + tableName);
        }
        final Comparator[] comparators = primaryKeySchema.getComparators();
        tableLocks = new ConcurrentSkipListMap<>((o1, o2) -> getComparatorForTableLocks(comparators, o1, o2));
        locks.get(dbName).put(tableName, tableLocks);
      }
    }
    return tableLocks;
  }

  private int getComparatorForTableLocks(Comparator[] comparators, Object[] o1, Object[] o2) {
    for (int i = 0; i < o1.length; i++) {
      if (o1[i] == null || o2[i] == null) {
        continue;
      }
      int value = comparators[i].compare(o1[i], o2[i]);
      if (value < 0) {
        return -1;
      }
      if (value > 0) {
        return 1;
      }
    }
    return 0;
  }

  private void doPreHandleTransaction(String tableName, String indexName, boolean isExplicitTrans, boolean isCommitting,
                                      long transactionId, Object[] primaryKey, AtomicBoolean shouldExecute,
                                      AtomicBoolean shouldDeleteLock, ConcurrentSkipListMap<Object[], RecordLock> tableLocks) {
    RecordLock lock = tableLocks.get(primaryKey);
    if (lock == null) {
      if (isExplicitTrans) {
        Transaction trans = transactions.computeIfAbsent(transactionId, k -> new Transaction(transactionId));
        lock = new RecordLock();
        lock.lockCount = 1;
        lock.transaction = trans;
        lock.tableName = tableName;
        lock.indexName = indexName;
        lock.primaryKey = primaryKey;
        trans.locks.add(lock);
        tableLocks.put(primaryKey, lock);
        shouldExecute.set(false);
      }
      else {
        shouldExecute.set(true);
      }
    }
    else if (isExplicitTrans) {
      if (lock.transaction.id != transactionId) {
        throw new RecordLockedException();
      }
      if (isCommitting) {
        shouldExecute.set(true);
        shouldDeleteLock.set(true);
      }
      else {
        lock.lockCount++;
      }
    }
    else {
      throw new RecordLockedException();
    }
  }
}
