package com.sonicbase.server;

import com.sonicbase.common.Record;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Responsible for
 */
public class TransactionManager {

  public enum OperationType {
    batchInsertWithRecord,
    insertWithRecord,
    batchInsert,
    insert,
    update,
    delete
  }

  private final DatabaseServer server;
  private ConcurrentHashMap<Long, Transaction> transactions = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentSkipListMap<Object[], RecordLock>>> locks = new ConcurrentHashMap<>();

  public TransactionManager(
      DatabaseServer databaseServer) {
    this.server = databaseServer;
  }

  public ConcurrentHashMap<Long, Transaction> getTransactions() {
    return transactions;
  }

  public ConcurrentHashMap<String, ConcurrentSkipListMap<Object[], RecordLock>> getLocks(String dbName) {
    return locks.get(dbName);
  }

  public Transaction getTransaction(long transactionId) {
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
    private OperationType type;
    private byte[] body;
    private String command;
    private boolean replayed;

    public Operation(OperationType type, String command, byte[] body, boolean replayedCommand) {
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
    private long id;
    private List<RecordLock> locks = new ArrayList<>();
    private ConcurrentHashMap<String, List<Record>> records = new ConcurrentHashMap<>();
    private List<Operation> operations = new ArrayList<>();

    public Transaction(long transactionId) {
      this.id = transactionId;
    }

    public ConcurrentHashMap<String, List<Record>> getRecords() {
      return records;
    }

    public void addOperation(OperationType type, String command, byte[] body, boolean replayedCommand) {
      operations.add(new Operation(type, command, body, replayedCommand));
    }

    public List<Operation> getOperations() {
      return operations;
    }

    public List<RecordLock> getLocks() {
      return locks;
    }
  }

  byte[] abortTransaction(String command, byte[] body) {
    String[] parts = command.split(":");
    String dbName = parts[5];
    long transactionId = Long.valueOf(parts[6]);

    synchronized (locks) {
      Transaction trans = transactions.get(transactionId);
      if (trans != null) {
        List<RecordLock> locks = trans.locks;
        for (RecordLock lock : locks) {
          String tableName = lock.tableName;
          Object[] primaryKey = lock.primaryKey;
          this.locks.get(tableName).remove(primaryKey);
        }
        transactions.remove(transactionId);
      }
    }
    return null;
  }

  public void deleteLock(String dbName, String tableName, String indexName, long transactionId, TableSchema tableSchema, Object[] primaryKey) {
    synchronized (locks) {
      ConcurrentSkipListMap<Object[], RecordLock> tableLocks = locks.get(dbName).get(tableName);
      RecordLock lock = tableLocks.get(primaryKey);
      lock.lockCount -= 1;
      if (lock.lockCount == 0) {
        tableLocks.remove(primaryKey);
        Transaction trans = transactions.get(transactionId);
        for (int i = 0; i < trans.locks.size(); i++) {
          lock = trans.locks.get(i);
          if (lock.tableName.equals(tableName)) {
            Comparator[] comparators = null;
            for (Map.Entry<String, IndexSchema> entry : tableSchema.getIndices().entrySet()) {
              if (entry.getValue().isPrimaryKey()) {
                comparators = entry.getValue().getComparators();
              }
            }
            boolean mismatch = false;
            for (int j = 0; j < primaryKey.length; j++) {
              if (comparators[j].compare(primaryKey[j], lock.primaryKey[j]) != 0) {
                mismatch = true;
                break;
              }
            }
            if (!mismatch) {
              trans.locks.remove(i);
              if (trans.locks.size() == 0) {
                transactions.remove(transactionId);
              }
              break;
            }
          }
        }
      }
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "EI_EXPOSE_REP2", justification = "copying the passed in data is too slow")
  @SuppressWarnings("PMD.ArrayIsStoredDirectly") //copying the passed in data is too slow
  public void preHandleTransaction(String dbName, String tableName, String indexName, boolean isExplicitTrans, boolean isCommitting, long transactionId, Object[] primaryKey,
                                   AtomicBoolean shouldExecute,
                                   AtomicBoolean shouldDeleteLock) {
    ConcurrentSkipListMap<Object[], RecordLock> tableLocks = null;
    if (!locks.containsKey(dbName)) {
      locks.put(dbName, new ConcurrentHashMap<String, ConcurrentSkipListMap<Object[], RecordLock>>());
    }
    tableLocks = locks.get(dbName).get(tableName);
    synchronized (locks) {
      if (tableLocks == null) {
        IndexSchema primaryKeySchema = null;
        for (Map.Entry<String, IndexSchema> entry : server.getCommon().getTables(dbName).get(tableName).getIndices().entrySet()) {
          if (entry.getValue().isPrimaryKey()) {
            primaryKeySchema = entry.getValue();
            break;
          }
        }
        final Comparator[] comparators = primaryKeySchema.getComparators();
        tableLocks = new ConcurrentSkipListMap<Object[], RecordLock>(new Comparator<Object[]>() {
          @Override
          public int compare(Object[] o1, Object[] o2) {
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
        });
        locks.get(dbName).put(tableName, tableLocks);
      }
    }
    RecordLock lock = tableLocks.get(primaryKey);
    if (lock == null) {
      if (isExplicitTrans) {
        Transaction trans = transactions.get(transactionId);
        if (trans == null) {
          trans = new Transaction(transactionId);
          transactions.put(transactionId, trans);
        }
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
