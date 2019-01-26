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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class IndexLookupTwoKeyTest {

  @Test
  public void testLookup() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);

    IndexLookupTwoKeys indexLookup = new IndexLookupTwoKeys(server);

    AtomicBoolean done = new AtomicBoolean();

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

    indexLookup.index = new Index(9010, new HashMap<Long, Boolean>(),  tableSchema, indexLookup.indexSchema.getName(), indexLookup.indexSchema.getComparators());



    int i = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[i]});
      indexLookup.index.put(key, address);
      i++;
    }

    indexLookup.setOriginalLeftKey(new Object[]{300L});
    indexLookup.setOriginalRightKey(new Object[]{700L});
    indexLookup.setLeftKey(null);
    indexLookup.setRightKey(null);
    indexLookup.setLeftOperator(BinaryExpression.Operator.GREATER);
    indexLookup.setRightOperator(BinaryExpression.Operator.LESS);
    indexLookup.count = 100;

    indexLookup.lookup();

    assertEquals(indexLookup.retRecords.get(0), records[2]);
    assertEquals(indexLookup.retRecords.get(1), records[3]);
    assertEquals(indexLookup.retRecords.get(2), records[4]);
    assertEquals(indexLookup.retRecords.size(), 3);

  }
}
