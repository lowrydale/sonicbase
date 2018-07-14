/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Collections;
import java.util.List;
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
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    IndexLookupTest.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);


    byte[][] records = IndexLookupTest.createRecords(common, tableSchema, 10);

    final AtomicBoolean calledLookup = new AtomicBoolean();
    when(client.send(eq("ReadManager:indexLookup"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
      calledLookup.set(true);
      ComObject retObj = new ComObject();
      ComArray array = retObj.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
      array = retObj.putArray(ComObject.Tag.keyRecords, ComObject.Type.byteArrayType);
      array = retObj.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);

      for (int i = 0; i < records.length; i++) {
        byte[] bytes = records[i];
        array.add(bytes);
      }

      retObj.put(ComObject.Tag.currOffset, records.length);
      retObj.put(ComObject.Tag.countReturned, records.length);

      return retObj.serialize();
    });

    CCJSqlParserManager parser = new CCJSqlParserManager();
    Select select = (Select) parser.parse(new StringReader("select * from table1 where field1 in(1, 2, 3)"));
    PlainSelect plainSelect = (PlainSelect)select.getSelectBody();
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
    retInExpression.setLimit(null);
    retInExpression.setOrderByExpressions(null);
    retInExpression.setParms(null);
    retInExpression.setReplica(1);
    retInExpression.setTableName("table1");
    retInExpression.setTopLevelExpression(column);
    retInExpression.setProbe(false);
    retInExpression.setViewVersion(10);


    ExpressionImpl.NextReturn ret = retInExpression.next(10, null, new AtomicLong(), new AtomicLong(), null, null, false,false, 0);

    Record[] recs = new Record[]{new Record("test", common, records[0])};
    assertFalse((Boolean) retInExpression.evaluateSingleRecord(new TableSchema[]{tableSchema}, recs, null));

    retInExpression.setNot(true);
    assertTrue((Boolean) retInExpression.evaluateSingleRecord(new TableSchema[]{tableSchema}, recs, null));

    assertEquals(retInExpression.toString(), "table1.field1 not in (1, 2, 3)");
    assertTrue(calledLookup.get());
  }
}
