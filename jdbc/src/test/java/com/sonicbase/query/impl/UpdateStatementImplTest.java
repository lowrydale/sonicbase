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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;
import org.testng.annotations.Test;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class UpdateStatementImplTest {

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

    final AtomicBoolean calledUpdate = new AtomicBoolean();
    when(client.send(eq("UpdateManager:updateRecord"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(invocationOnMock -> {
      calledUpdate.set(true);
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
    Update update = (Update) parser.parse(new StringReader("update table1 set field1=1, field2='1' where field1=0"));

    UpdateStatementImpl updateStatement = new UpdateStatementImpl(client);
    AtomicInteger currParmNum = new AtomicInteger();
    //todo: support multiple tables?
    updateStatement.setTableName(update.getTables().get(0).getName());

    List<Column> columns = update.getColumns();
    for (Column column : columns) {
      updateStatement.addColumn(column);
    }
    ParameterHandler parms = new ParameterHandler();
    List<Expression> expressions = update.getExpressions();
    for (Expression expression : expressions) {
      ExpressionImpl qExpression = SelectStatementHandler.getExpression(client, currParmNum, expression, updateStatement.getTableName(), parms);
      updateStatement.addSetExpression(qExpression);
    }

    ExpressionImpl whereExpression = SelectStatementHandler.getExpression(client, currParmNum, update.getWhere(), updateStatement.getTableName(), parms);
    whereExpression.setReplica(0);
    updateStatement.setWhereClause(whereExpression);

    updateStatement.execute("test", "update table1 set field1=1, field2='1' where field1=0",
        null, 100L, 100L, (short)100, false, null, 0);

    assertTrue(calledLookup.get());
    assertTrue(calledUpdate.get());
  }
}
