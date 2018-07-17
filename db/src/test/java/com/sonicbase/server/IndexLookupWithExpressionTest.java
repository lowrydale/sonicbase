/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.BinaryExpressionImpl;
import com.sonicbase.query.impl.ColumnImpl;
import com.sonicbase.query.impl.ConstantImpl;
import com.sonicbase.schema.DataType;
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

public class IndexLookupWithExpressionTest {

  @Test
  public void testLookup() throws UnsupportedEncodingException {
    com.sonicbase.server.DatabaseServer server = mock(DatabaseServer.class);
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);

    IndexLookupWithExpression indexLookup = new IndexLookupWithExpression(server);
    indexLookup.setDbName("test");
    AtomicBoolean done = new AtomicBoolean();

    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    indexLookup.indexSchema = TestUtils.createIndexSchema(tableSchema);
    indexLookup.setTableSchema(tableSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    when(server.getCommon()).thenReturn(common);

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

    indexLookup.setLeftKey(new Object[]{300L});
    indexLookup.setRightKey(new Object[]{700L});
    indexLookup.setLeftOperator(BinaryExpression.Operator.greater);
    indexLookup.setRightOperator(BinaryExpression.Operator.less);
    indexLookup.count = 100;

    BinaryExpressionImpl expression = new BinaryExpressionImpl();
    ColumnImpl leftExpression = new ColumnImpl();
    leftExpression.setColumnName("field1");
    leftExpression.setTableName(tableSchema.getName());
    expression.setLeftExpression(leftExpression);
    ConstantImpl rightExpression = new ConstantImpl();
    rightExpression.setValue(300L);
    rightExpression.setSqlType(DataType.Type.BIGINT.getValue());
    expression.setRightExpression(rightExpression);
    expression.setOperator(BinaryExpression.Operator.equal);

    indexLookup.setExpression(expression);

    indexLookup.lookup();

    assertEquals(indexLookup.retRecords.get(0), records[1]);
    assertEquals(indexLookup.retRecords.size(), 1);

  }
}
