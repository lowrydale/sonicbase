/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import org.testng.annotations.Test;

import java.io.*;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class DatabaseCommonTest {

  @Test
  public void testKeySerialization() throws UnsupportedEncodingException, EOFException {

    DatabaseCommon common = new DatabaseCommon();
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createSecondaryIndexSchema(tableSchema);
    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeysForSecondaryIndex(10);

    byte[] bytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0));

    Object[] key = DatabaseCommon.deserializeKey(tableSchema, bytes);

    assertEquals(compareKeys(indexSchema, keys, key), 0);
  }

  @Test
  void testKeySerialationPrep() throws IOException {
    DatabaseCommon common = new DatabaseCommon();
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createSecondaryIndexSchema(tableSchema);
    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeysForSecondaryIndex(10);

    byte[] bytes = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), keys.get(0));

    DataType.Type[] types = DatabaseCommon.deserializeKeyPrep(tableSchema, bytes);

    Object[] key = DatabaseCommon.deserializeKey(tableSchema, types, new DataInputStream(new ByteArrayInputStream(bytes)));

    assertEquals(compareKeys(indexSchema, keys, key), 0);
  }

  @Test
  void testKeySerialationTyped() throws IOException {
    DatabaseCommon common = new DatabaseCommon();
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createSecondaryIndexSchema(tableSchema);
    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    List<Object[]> keys = IndexLookupTest.createKeysForSecondaryIndex(10);

    byte[] bytes = DatabaseCommon.serializeTypedKey(keys.get(0));

    Object[] key = DatabaseCommon.deserializeTypedKey(bytes);

    assertEquals(compareKeys(indexSchema, keys, key), 0);
  }

  private int compareKeys(IndexSchema indexSchema, List<Object[]> keys, Object[] key) {
    for (int i = 0; i < key.length; i++) {
      int value = indexSchema.getComparators()[i].compare(key[i], keys.get(0)[i]);
      if (value < 0) {
        System.out.println("failed field=" + i);
        return -1;
      }
      if (value > 0) {
        System.out.println("failed field=" + i);
        return 1;
      }
    }
    return 0;
  }
}