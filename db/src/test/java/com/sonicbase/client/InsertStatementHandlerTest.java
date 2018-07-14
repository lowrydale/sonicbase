/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.jdbcdriver.QueryType;
import com.sonicbase.query.impl.InsertStatementImpl;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class InsertStatementHandlerTest {

  @Test
  public void test() throws SQLException, JSQLParserException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    IndexLookupTest.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    AtomicBoolean calledWithRecord = new AtomicBoolean();
    when(client.send(eq("UpdateManager:insertIndexEntryByKeyWithRecord"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(
        (Answer) invocationOnMock ->{ calledWithRecord.set(true);
        ComObject ret = new ComObject();
        ret.put(ComObject.Tag.count, 1);
        return ret.serialize();});

    AtomicBoolean calledWithoutRecord = new AtomicBoolean();
    when(client.send(eq("UpdateManager:insertIndexEntryByKey"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(
        (Answer) invocationOnMock ->{ calledWithoutRecord.set(true);
          ComObject ret = new ComObject();
          ret.put(ComObject.Tag.count, 1);
          return ret.serialize();});

    CCJSqlParserManager parser = new CCJSqlParserManager();
    Insert insert = (Insert) parser.parse(new StringReader("insert into table1 (field1, field2) values (1, '1')"));

    InsertStatementHandler handler = new InsertStatementHandler(client);
    handler.execute("test", null, "insert into table1 (field1, field2) values (1, '1')", insert, null, 100L, 100L, (short)100, false, null, 0);

    assertTrue(calledWithRecord.get());
    assertTrue(calledWithoutRecord.get());
  }

  @Test
  public void testConvertToUpdate() throws SQLException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    IndexLookupTest.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    final AtomicBoolean called = new AtomicBoolean();
    when(client.executeQuery(anyString(), anyObject(), anyString(), anyObject(), anyBoolean(),
        eq(null), eq(null), eq(null), eq(false), eq(null), eq(true))).thenAnswer(
        (Answer) invocationOnMock ->{
          Object[] args = invocationOnMock.getArguments();
          called.set(true); assertEquals((String)args[2], "update table1 set field1=? , field2=?  where field1=? ");
          return 1;});

    InsertStatementImpl statement = new InsertStatementImpl(client);
    statement.addValue("field1", 1L);
    statement.addValue("field2", "1");
    statement.setTableName("table1");

    InsertStatementHandler handler = new InsertStatementHandler(client);
    handler.convertInsertToUpdate("test", statement);

    assertTrue(called.get());
  }
}
