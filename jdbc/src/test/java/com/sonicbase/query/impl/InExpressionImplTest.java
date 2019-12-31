package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ClientTestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.*;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class InExpressionImplTest {

  @Test
  public void test() throws UnsupportedEncodingException, JSQLParserException {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      DatabaseClient client = mock(DatabaseClient.class);
      when(client.getExecutor()).thenReturn(executor);
      DatabaseCommon common = new DatabaseCommon();
      when(client.getCommon()).thenReturn(common);
      TableSchema tableSchema = ClientTestUtils.createTable();
      IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
      ClientTestUtils.createStringIndexSchema(tableSchema);
      common.getTables("test").put(tableSchema.getName(), tableSchema);
      common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
      when(client.getShardCount()).thenReturn(2);

      byte[][] records = ClientTestUtils.createRecords(common, tableSchema, 10);

      final AtomicBoolean calledLookup = new AtomicBoolean();
      when(client.send(eq("ReadManager:batchIndexLookup"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
        calledLookup.set(true);
        ComObject retObj = new ComObject(6);
        ComArray array = retObj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE, records.length);
        array = retObj.putArray(ComObject.Tag.KEY_RECORDS, ComObject.Type.BYTE_ARRAY_TYPE, records.length);
        array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE, records.length);

        for (int i = 0; i < records.length; i++) {
          byte[] bytes = records[i];
          array.add(bytes);
        }

        retObj.put(ComObject.Tag.CURR_OFFSET, (long)records.length);
        retObj.put(ComObject.Tag.COUNT_RETURNED, (long)records.length);

        retObj.putArray(ComObject.Tag.RET_KEYS, ComObject.Type.OBJECT_TYPE, 1);

        return retObj;
      });

      CCJSqlParserManager parser = new CCJSqlParserManager();
      Select select = (Select) parser.parse(new StringReader("select * from table1 where field1 in(1, 2, 3)"));
      PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
      Expression whereExpression = plainSelect.getWhere();
      InExpressionImpl retInExpression = new InExpressionImpl(client, null, "table1");
      net.sf.jsqlparser.expression.operators.relational.InExpression inExpression = (net.sf.jsqlparser.expression.operators.relational.InExpression) whereExpression;
      retInExpression.setNot(inExpression.isNot());
      retInExpression.setLeftExpression(SelectStatementHandler.getExpression(client, new AtomicInteger(), inExpression.getLeftExpression(), "table1", null));
      ItemsList items = inExpression.getRightItemsList();
      if (items instanceof ExpressionList) {
        ExpressionList expressionList = (ExpressionList) items;
        List expressions = expressionList.getExpressions();
        for (Object obj : expressions) {
          retInExpression.addExpression(SelectStatementHandler.getExpression(client, new AtomicInteger(), (Expression) obj, "table1", null));
        }
      }
      retInExpression.setDbName("test");
      retInExpression.setTableName("table1");
      retInExpression.setRecordCache(new ExpressionImpl.RecordCache());

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytesOut);
      retInExpression.serialize((short) 100, out);

      retInExpression = new InExpressionImpl();
      retInExpression.deserialize((short) 100, new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));
      retInExpression.setClient(client);
      retInExpression.setDbName("test");
      retInExpression.setTableName("table1");
      retInExpression.setRecordCache(new ExpressionImpl.RecordCache());

      ColumnImpl column = new ColumnImpl();
      column.setColumnName("field1");
      column.setTableName("table1");
      retInExpression.setColumns(Collections.<ColumnImpl>singletonList(column));

      retInExpression.setCounters(null);
      retInExpression.setDbName("test");
      retInExpression.setDebug(false);
      retInExpression.setOrderByExpressions(null);
      retInExpression.setParms(null);
      retInExpression.setReplica(1);
      retInExpression.setTableName("table1");
      retInExpression.setTopLevelExpression(column);
      retInExpression.setProbe(false);
      retInExpression.setViewVersion(10);


      AtomicBoolean didTableScan = new AtomicBoolean();
      ExpressionImpl.NextReturn ret = retInExpression.next(null, 10, null, new AtomicLong(),
          new AtomicLong(), null, null, false, false, 0, didTableScan);

      Record[] recs = new Record[]{new Record("test", common, records[0])};
      assertFalse((Boolean) retInExpression.evaluateSingleRecord(new TableSchema[]{tableSchema}, recs, null));

      retInExpression.setNot(true);
      assertTrue((Boolean) retInExpression.evaluateSingleRecord(new TableSchema[]{tableSchema}, recs, null));

      assertEquals(retInExpression.toString(), "table1.field1 not in (1, 2, 3)");
      assertTrue(calledLookup.get());
    }
    finally {
      executor.shutdownNow();
    }
  }
}
