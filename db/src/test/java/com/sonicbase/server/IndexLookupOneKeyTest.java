package com.sonicbase.server;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class IndexLookupOneKeyTest {

  @Test
  public void testLookup() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);

    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    indexLookup.indexSchema = TestUtils.createIndexSchema(tableSchema);
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    indexLookup.currOffset = new AtomicLong();
    indexLookup.countReturned = new AtomicLong();
    indexLookup.retRecords = new ArrayList<>();
    indexLookup.retKeyRecords = new ArrayList<>();

    indexLookup.index = new Index(tableSchema, indexLookup.indexSchema.getName(), indexLookup.indexSchema.getComparators());

    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      indexLookup.index.put(key, address);
      i++;
    }

    indexLookup.setOriginalLeftKey(new Object[]{400L});
    indexLookup.setLeftKey(null);
    indexLookup.setLeftOperator(BinaryExpression.Operator.LESS);
    indexLookup.count = 100;

    indexLookup.lookup();

  //  assertEquals(indexLookup.retRecords.get(0), records[0]);
//    assertEquals(indexLookup.retRecords.get(1), records[1]);
    assertEquals(indexLookup.retRecords.size(), 2);

  }

  @Test
  public void testLookupDescend() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);

    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    indexLookup.indexSchema = TestUtils.createIndexSchema(tableSchema);
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    indexLookup.currOffset = new AtomicLong();
    indexLookup.countReturned = new AtomicLong();
    indexLookup.retRecords = new ArrayList<>();
    indexLookup.retKeyRecords = new ArrayList<>();

    indexLookup.setAscending(false);

    indexLookup.index = new Index(tableSchema, indexLookup.indexSchema.getName(), indexLookup.indexSchema.getComparators());



    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      indexLookup.index.put(key, address);
      i++;
    }

    indexLookup.setOriginalLeftKey(new Object[]{400L});
    indexLookup.setLeftKey(null);
    indexLookup.setLeftOperator(BinaryExpression.Operator.LESS);
    indexLookup.count = 100;

    indexLookup.lookup();

    assertEquals(indexLookup.retRecords.get(1), records[0]);
    assertEquals(indexLookup.retRecords.get(0), records[1]);
    assertEquals(indexLookup.retRecords.size(), 2);

  }

  @Test
  public void testLookupNonUnique() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);

    IndexLookupOneKey indexLookup = new IndexLookupOneKey(server);

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTableNonUnique();
    indexLookup.indexSchema = TestUtils.createIndexSchemaNonUnique(tableSchema, 1);
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);

    byte[][] records = TestUtils.createNonUniqueRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createNonUniqueKeys(10);

    indexLookup.currOffset = new AtomicLong();
    indexLookup.countReturned = new AtomicLong();
    indexLookup.retRecords = new ArrayList<>();
    indexLookup.retKeyRecords = new ArrayList<>();

    indexLookup.setAscending(false);

    indexLookup.index = new Index(tableSchema, indexLookup.indexSchema.getName(), indexLookup.indexSchema.getComparators());



    byte[][] currRecords = new byte[][]{records[0], records[3], records[6], records[9]};
    Object address = addressMap.toUnsafeFromRecords(currRecords);
    indexLookup.index.put(new Object[]{200L}, address);

    currRecords = new byte[][]{records[1], records[4], records[7]};
    address = addressMap.toUnsafeFromRecords(currRecords);
    indexLookup.index.put(new Object[]{201L}, address);

    currRecords = new byte[][]{records[2], records[5], records[8]};
    address = addressMap.toUnsafeFromRecords(currRecords);
    indexLookup.index.put(new Object[]{202L}, address);

    indexLookup.setOriginalLeftKey(new Object[]{200L});
    indexLookup.setLeftKey(null);
    indexLookup.setLeftOperator(BinaryExpression.Operator.EQUAL);
    indexLookup.count = 100;

    indexLookup.lookup();

    assertEquals(indexLookup.retRecords.get(0), records[0]);
    assertEquals(indexLookup.retRecords.get(1), records[3]);
    assertEquals(indexLookup.retRecords.get(2), records[6]);
    assertEquals(indexLookup.retRecords.get(3), records[9]);
    assertEquals(indexLookup.retRecords.size(), 4);

  }
}
