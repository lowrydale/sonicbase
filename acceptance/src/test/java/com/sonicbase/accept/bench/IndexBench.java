/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.accept.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.index.Index;
import com.sonicbase.index.NativePartitionedTreeImpl;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class IndexBench {

  private static NativePartitionedTreeImpl nativeIndex;
  private static Index index;

  public static TableSchema createTable() {
    TableSchema tableSchema = new TableSchema();
    tableSchema.setName("table1");
    tableSchema.setTableId(100);
    List<FieldSchema> fields = new ArrayList<>();
    FieldSchema fSchema = new FieldSchema();
    fSchema.setName("_id");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field1");
    fSchema.setType(DataType.Type.BIGINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field2");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field3");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field4");
    fSchema.setType(DataType.Type.INTEGER);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field5");
    fSchema.setType(DataType.Type.SMALLINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field6");
    fSchema.setType(DataType.Type.TINYINT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field7");
    fSchema.setType(DataType.Type.CHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field8");
    fSchema.setType(DataType.Type.NCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field9");
    fSchema.setType(DataType.Type.FLOAT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field10");
    fSchema.setType(DataType.Type.REAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field11");
    fSchema.setType(DataType.Type.DOUBLE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field12");
    fSchema.setType(DataType.Type.BOOLEAN);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field13");
    fSchema.setType(DataType.Type.BIT);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field14");
    fSchema.setType(DataType.Type.VARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field15");
    fSchema.setType(DataType.Type.CLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field16");
    fSchema.setType(DataType.Type.NCLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field17");
    fSchema.setType(DataType.Type.LONGVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field18");
    fSchema.setType(DataType.Type.NVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field19");
    fSchema.setType(DataType.Type.LONGNVARCHAR);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field20");
    fSchema.setType(DataType.Type.LONGVARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field21");
    fSchema.setType(DataType.Type.VARBINARY);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field22");
    fSchema.setType(DataType.Type.BLOB);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field23");
    fSchema.setType(DataType.Type.NUMERIC);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field24");
    fSchema.setType(DataType.Type.DECIMAL);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field25");
    fSchema.setType(DataType.Type.DATE);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field26");
    fSchema.setType(DataType.Type.TIME);
    fields.add(fSchema);
    fSchema = new FieldSchema();
    fSchema.setName("field27");
    fSchema.setType(DataType.Type.TIMESTAMP);
    fields.add(fSchema);
    tableSchema.setFields(fields);
    List<String> primaryKey = new ArrayList<>();
    primaryKey.add("field1");
    tableSchema.setPrimaryKey(primaryKey);
    return tableSchema;
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema) {
    return createIndexSchema(tableSchema, 1);
  }

  public static IndexSchema createIndexSchema(TableSchema tableSchema, int partitionCount) {
    IndexSchema indexSchema = new IndexSchema();
    indexSchema.setFields(new String[]{"field1"}, tableSchema);
    indexSchema.setIndexId(1);
    indexSchema.setIsPrimaryKey(true);
    indexSchema.setName("_primarykey");
    indexSchema.setComparators(tableSchema.getComparators(new String[]{"field1"}));

    TableSchema.Partition[] partitions = new TableSchema.Partition[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      partitions[i] = new TableSchema.Partition();
      partitions[i].setUnboundUpper(true);
    }
    indexSchema.setCurrPartitions(partitions);
    tableSchema.addIndex(indexSchema);
    return indexSchema;
  }

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    //Thread.sleep(10000000);
    TableSchema tableSchema = createTable();
    createIndexSchema(tableSchema);

    index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, "_primarykey", new Comparator[]{DataType.getLongComparator()});
    nativeIndex = (NativePartitionedTreeImpl) index.getImpl();
    new IndexBench().bench();
  }

  private static int BLOCK_SIZE = DatabaseClient.SELECT_PAGE_SIZE;

  private final Unsafe unsafe = getUnsafe();
  public static Unsafe getUnsafe() {
    try {
      Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
      singleoneInstanceField.setAccessible(true);
      return (Unsafe) singleoneInstanceField.get(null);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final Object[] mutexes = new Object[100_000];

  private void bench() throws ExecutionException, InterruptedException {
    boolean mixg = false;
    boolean serial = false;
    getUnsafe();
    for (int i = 0; i < mutexes.length; i++) {
      mutexes[i] = new Object();
    }

    ThreadPoolExecutor executor = new ThreadPoolExecutor(32, 32, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor2 = new ThreadPoolExecutor(128, 128, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    ThreadPoolExecutor executor3 = new ThreadPoolExecutor(24, 24, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    final AtomicInteger countInserted = new AtomicInteger();
    final AtomicLong countRead = new AtomicLong();
    final List<Future> futures = new ArrayList<>();
    final AtomicLong last = new AtomicLong(System.currentTimeMillis());
    final long begin = System.currentTimeMillis();

    final AtomicLong localCountInserted = new AtomicLong();
    List<Future> futures2 = new ArrayList<>();
    long beginInsert = System.currentTimeMillis();
    for (int i = 0; i < 4; i++) {
      final int offset = i;
      futures2.add(executor3.submit((Callable) () -> {
        for (int i1 = 0; i1 < 10_000_000; i1++) {
          doInsert(countInserted, begin, localCountInserted, offset, i1);
        }
        return null;
      }));
    }
    if (serial) {
      for (Future future : futures2) {
        future.get();
      }
    }
    System.out.println("insert rate=" + (100_000_000d / System.currentTimeMillis() / (System.currentTimeMillis() - beginInsert) * 1000f));

    futures2.clear();
    localCountInserted.set(0);
//    for (int i = 0; i < 4; i++) {
//      final int offset = i;
//      futures2.add(executor3.submit((Callable) () -> {
//        for (int i1 = 0; i1 < 100_000_000; i1++) {
//          doDelete(countInserted, begin, localCountInserted, offset, i1);
//        }
//        return null;
//      }));
//    }
//    if (serial) {
//      for (Future future : futures2) {
//        future.get();
//      }
//    }
    System.out.println("delete rate=" + (100_000_000d / System.currentTimeMillis() / (System.currentTimeMillis() - beginInsert) * 1000f));

    for (int i = 0; i < 32; i++) {
      final int currIndex = i;
      futures.add(executor.submit((Callable) () -> {

        if (true  ) {
          futures2.add(executor2.submit((Callable) () -> {
            try {
              Thread.sleep(5_000);
//          (NativePartitionedTree.this) {
              long[] values = new long[BLOCK_SIZE + 200];
              Object[][] keys = new Object[BLOCK_SIZE + 200][];
              for (int j = 0; j < keys.length; j++) {
                keys[j] = new Object[1];
              }
              for (int j = 0; j < (mixg ? 1000 : 100_000); j++) {
                if (mixg) {
                  for (int i1 = 0; i1 < localCountInserted.get() - 1000; i1++) {
                    byte[] keyBytes = new byte[8];
                    writeLong(keyBytes, i1);
                    long value = (long) nativeIndex.get(new Object[]{i1});
                    if (value != i1) {
                      System.out.println("key mismatch: expected=" + i1 + ", actual=" + value);
                    }
                    long count = countRead.addAndGet(1);
                    //(mutex) {
                    if (System.currentTimeMillis() - last.get() > 1_000) {
                      last.set(System.currentTimeMillis());
                      System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                      System.out.flush();
                    }
                    //}
                  }
                }
                else if (false) {

                  boolean first = true;
                  Object[] lastKey = new Object[]{0L};
                  System.out.println("starting over");
                  for (int i1 = 0; i1 < localCountInserted.get() /*- 1000000*/; i1 += BLOCK_SIZE) {
                    byte[] keyBytes = new byte[8];
                    writeLong(keyBytes, (long) lastKey[0]);
                    int retCount = 0;
                    if (true) {
                      if (true) {
                        retCount = nativeIndex.tailBlock(lastKey, BLOCK_SIZE, first, keys, values);
                        //                      retCount = BLOCK_SIZE;
                        if (retCount == 0) {
                          System.out.println("breaking in harness");
                          break;
                        }
                        lastKey = keys[retCount - 1];
//                        long[] values = new long[retCount];
//                        for (int m = 0; m < retCount; m++) {
//                          if (i1 >  1_000_000 - 1024) {
//                            System.out.println(results[m]);
//                          }
//                          keys[m] = new Object[]{results[m]};
//                          values[m] = results[m + retCount];
//                          long address = (long) values[m];
//                          byte[] bytes = new byte[75];
//                          for (int l = 0; l < 75; l++) {
//                            bytes[l] = unsafe.getByte(address + l);
//                          }
//                        }
                        if (retCount < BLOCK_SIZE) {
                          System.out.println("returning fewer");
                          break;
                        }
                      }
                      else {
//                        byte[] results = tailBlock(indices[0], keyBytes, BLOCK_SIZE, first);
//                        long[] values = new long[BLOCK_SIZE + 200];
//                        retCount = parseResults(results, keys, values);
//                        for (int k = 0; k < retCount; k++) {
//                          long address = (long)values[k];
//                          byte[] bytes = new byte[75];
//                          for (int l = 0; l < 75; l++) {
//                            bytes[l] = unsafe.getByte(address + l);
//                          }
//                        }
                      }
                    }
                    else {
//                      byte[][] values = new byte[BLOCK_SIZE + 200][];
//                      retCount = tailBlockBytes(new Object[]{((long)i1) - 1}, BLOCK_SIZE, first, keys, values);
//                      for (int k = 0; k < retCount; k++) {
//                        if (values[k].length != 75) {
//                          System.out.println("size mismatch");
//                        }
//                      }

                    }
                    countRead.addAndGet(retCount);
//                    for (int keyOffset = 0; keyOffset < retCount; keyOffset++) {
//                      if ((long)keys[keyOffset][0] != i1 + keyOffset) {
//                        System.out.println("key mismatch: expected=" + (i1 + keyOffset) + ", actual=" + keys[keyOffset][0]);
//                      }
//                    }

                    //(mutex) {
                    if (System.currentTimeMillis() - last.get() > 1_000) {
                      last.set(System.currentTimeMillis());
                      System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                      System.out.flush();
                    }
                    first = false;
                    //}
                  }
                }
                else {
                  Object[] lastKey = new Object[]{0L};
                  System.out.println("starting over");
                  for (int i1 = 0; i1 < localCountInserted.get() /*- 1000000*/; i1 += BLOCK_SIZE) {
                    index.higherEntry(new Object[]{0L});
                    final AtomicInteger count = new AtomicInteger();
                    index.visitTailMap(lastKey, new Index.Visitor() {
                      @Override
                      public boolean visit(Object[] key, Object value) {
                        countRead.addAndGet(1);

                        if (System.currentTimeMillis() - last.get() > 1_000) {
                          last.set(System.currentTimeMillis());
                          System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
                          System.out.flush();
                        }
                        if (count.incrementAndGet() >= BLOCK_SIZE) {
                          return false;
                        }
                        return true;
                      }
                    });
                  }
                }
              }
              for (Future future : futures2) {
                future.get();
              }
              System.out.println("read progress: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
              System.out.flush();
//          }
            }
            catch (Exception e) {
              e.printStackTrace();
            }
            return null;
          }));
        }
        return null;
      }));
    }

    for (Future future : futures) {
      future.get();
    }

    Thread.sleep(10000000);

    System.out.println("read progress - finished: count=" + countRead.get() + ", rate=" + ((float) countRead.get() / (System.currentTimeMillis() - begin) * 1000f));
    System.out.flush();

    executor.shutdownNow();
    executor2.shutdownNow();
  }

  private void doDelete(AtomicInteger countInserted, long begin, AtomicLong localCountInserted, int offset, long i1) {
    if (i1 % 4 == offset) {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, i1);

      nativeIndex.remove(new Object[]{i1});

      localCountInserted.incrementAndGet();
      if (countInserted.incrementAndGet() % 1_000_000 == 0) {
        System.out.println("Delete progress: count=" + countInserted.get() + ", rate=" + ((float) countInserted.get() / (System.currentTimeMillis() - begin) * 1000f));
      }
    }
  }

  private void doInsert(AtomicInteger countInserted, long begin, AtomicLong localCountInserted, int offset, long i1) {
    if (i1 % 4 == offset) {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, i1);

      if (true) {
        long address = unsafe.allocateMemory(75);
        for (int l = 0; l < 75; l++) {
          unsafe.putByte(address + l, (byte) 0);
        }

        nativeIndex.put(new Object[]{i1}, address);

        //                          if (i1 % 150 == 0) {
        //                Thread.sleep(5);
        //              }
        localCountInserted.incrementAndGet();
        if (countInserted.incrementAndGet() % 1_000_000 == 0) {
          System.out.println("insert progress: count=" + countInserted.get() + ", rate=" + ((float) countInserted.get() / (System.currentTimeMillis() - begin) * 1000f));
        }
      }
    }
  }


  public final long readLong(byte[] bytes, int offset) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (bytes[i + offset] & 0xFF);
    }
    return result;
  }

  public final void writeLong(byte[] bytes, long v) {
    bytes[0] = (byte)(v >>> 56);
    bytes[1] = (byte)(v >>> 48);
    bytes[2] = (byte)(v >>> 40);
    bytes[3] = (byte)(v >>> 32);
    bytes[4] = (byte)(v >>> 24);
    bytes[5] = (byte)(v >>> 16);
    bytes[6] = (byte)(v >>>  8);
    bytes[7] = (byte)(v >>>  0);
  }

  public final int readInt(byte[] bytes, int offset) throws IOException {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
//    int ch1 = bytes[offset.get()];
//    int ch2 = bytes[offset.get() + 1];
//    int ch3 = bytes[offset.get() + 1];
//    int ch4 = bytes[offset.get() + 1];
//    if ((ch1 | ch2 | ch3 | ch4) < 0)
//      throw new EOFException();
//    offset.addAndGet(4);
//    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }


}
