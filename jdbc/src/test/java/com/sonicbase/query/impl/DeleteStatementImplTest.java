package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.ComArray;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.delete.Delete;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class DeleteStatementImplTest {

  @Test
  public void test() throws JSQLParserException, UnsupportedEncodingException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    TestUtils.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    final AtomicBoolean calledDeleteRecord = new AtomicBoolean();
    when(client.send(eq("UpdateManager:deleteRecord"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
      calledDeleteRecord.set(true);
      return null;
    });
    final AtomicBoolean calledDeleteIndex = new AtomicBoolean();
    when(client.send(eq("UpdateManager:deleteIndexEntry"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
      calledDeleteIndex.set(true);
      return null;
    });

    final AtomicBoolean calledLookup = new AtomicBoolean();
    when(client.send(eq("ReadManager:indexLookup"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
      calledLookup.set(true);
      ComObject retObj = new ComObject();
      ComArray array = retObj.putArray(ComObject.Tag.KEYS, ComObject.Type.BYTE_ARRAY_TYPE);
      array = retObj.putArray(ComObject.Tag.KEY_RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);
      array = retObj.putArray(ComObject.Tag.RECORDS, ComObject.Type.BYTE_ARRAY_TYPE);

      for (int i = 0; i < records.length; i++) {
        byte[] bytes = records[i];
        array.add(bytes);
      }

      retObj.put(ComObject.Tag.CURR_OFFSET, records.length);
      retObj.put(ComObject.Tag.COUNT_RETURNED, records.length);

      return retObj.serialize();
    });
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Delete delete = (Delete) parser.parse(new StringReader("delete from table1 where field1=0"));

    DeleteStatementImpl deleteStatement = new DeleteStatementImpl(client);
    AtomicInteger currParmNum = new AtomicInteger();
    //todo: support multiple tables?
    deleteStatement.setTableName(delete.getTable().getName());

    ParameterHandler parms = new ParameterHandler();

    ExpressionImpl whereExpression = SelectStatementHandler.getExpression(client, currParmNum, delete.getWhere(), delete.getTable().getName(), parms);
    whereExpression.setReplica(0);
    deleteStatement.setWhereClause(whereExpression);

    deleteStatement.execute("test", "delete from table1 where field1=0",
        null, 100L, 100L, (short)100, false, null, 0);

    assertTrue(calledLookup.get());
    assertTrue(calledDeleteRecord.get());
//    assertTrue(calledDeleteIndex.get());
  }

}
