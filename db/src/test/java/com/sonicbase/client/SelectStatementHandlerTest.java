/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.common.*;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.Expression;
import com.sonicbase.query.ResultSet;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SetOperationList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SelectStatementHandlerTest {

  private DatabaseCommon common;
  private ServersConfig serversConfig;
  private DatabaseClient client;
  private byte[][] records;

  @BeforeMethod
  public void beforeMethod() throws IOException {
    common = new DatabaseCommon();
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    },\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10010,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10020,\n" +
        "          \"httpPort\": 8080\n" +
        "        },\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 10030,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode) node).withArray("shards"), 1, true, true);
    common.setServersConfig(serversConfig);

    final AtomicInteger callCount = new AtomicInteger();
    client = new DatabaseClient("localhost", 9010, 0, 0, false, common, null) {

      public byte[] sendToMaster(ComObject body) {
        return null;
      }

      public byte[] send(String method,
                         int shard, long auth_user, ComObject body, Replica replica) {
        if (method.equals("DatabaseServer:getConfig")) {
          callCount.incrementAndGet();
        }

        if (method.equals("ReadManager:indexLookupExpression")) {
          return new ComObject().serialize();
        }
        else if (method.equals("ReadManager:indexLookup")) {
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
        }
        else if (method.equals("ReadManager:batchIndexLookup")) {
          ComObject retObj = new ComObject();
          ComArray retKeysArray = retObj.putArray(ComObject.Tag.retKeys, ComObject.Type.objectType);

          ComArray keys = body.getArray(ComObject.Tag.keys);
          for (Object keyObj : keys.getArray()) {
            ComObject key = (ComObject) keyObj;

            Object[] leftKey = new Object[]{key.getLong(ComObject.Tag.longKey)};

            for (int i = 0; i < records.length; i++) {
              Record record = new Record("test", common, records[i]);
              if ((long) record.getFields()[1] == (long) leftKey[0]) {
                ComObject retEntry = new ComObject();
                retKeysArray.add(retEntry);

                retEntry.put(ComObject.Tag.offset, i);
                retEntry.put(ComObject.Tag.keyCount, 0);

                ComArray keysArray = retEntry.putArray(ComObject.Tag.keyRecords, ComObject.Type.byteArrayType);
                keysArray = retEntry.putArray(ComObject.Tag.keys, ComObject.Type.byteArrayType);
                ComArray retRecordsArray = retEntry.putArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
                byte[] bytes = records[i];
                retRecordsArray.add(bytes);
              }
            }
          }
          return retObj.serialize();
        }
        else if (method.equals("ReadManager:serverSelect")) {

          ComObject retObj = new ComObject();
          retObj.put(ComObject.Tag.legacySelectStatement, body.getByteArray(ComObject.Tag.legacySelectStatement));

          ComArray tableArray = retObj.putArray(ComObject.Tag.tableRecords, ComObject.Type.arrayType);
          outer:
          for (byte[] record : records) {
            ComArray recordArray = tableArray.addArray(ComObject.Tag.records, ComObject.Type.byteArrayType);
            recordArray.add(record);
          }
          return retObj.serialize();
        }
        return null;
      }

      public String getCluster() {
        return "test";
      }
    };

    client.setClientStatsHandler(new ClientStatsHandler(client) {
      public byte[] sendToMasterOnSharedClient(ComObject cobj, DatabaseClient sharedClient) {
        return null;
      }
    });

    TableSchema tableSchema = IndexLookupTest.createTable();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);

    records = IndexLookupTest.createRecords(common, tableSchema, 10);

    tableSchema = IndexLookupTest.createTable2();
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
    indexSchema = IndexLookupTest.createIndexSchema(tableSchema);


  }


  @Test
  public void testBasic() throws JSQLParserException {

    String sql = "select * from table1 where id < 10.1 and id > '1'";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    assertEquals(selectStatement.getFromTable(), "table1");
    BinaryExpressionImpl where = (BinaryExpressionImpl) selectStatement.getWhereClause();
    assertEquals(((BinaryExpressionImpl) where.getLeftExpression()).getOperator(), BinaryExpression.Operator.less);
    assertEquals(((BinaryExpressionImpl) where.getRightExpression()).getOperator(), BinaryExpression.Operator.greater);
  }

  @Test
  public void testOr() throws JSQLParserException {
    String sql = "select * from table1 where id < 10 or id > 1";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    assertEquals(selectStatement.getFromTable(), "table1");
    BinaryExpressionImpl where = (BinaryExpressionImpl) selectStatement.getWhereClause();
    assertEquals(where.getOperator(), BinaryExpression.Operator.or);

  }

  @Test
  public void testIn() throws JSQLParserException {

    String sql = "select * from table1 where id not in (3, 4) order by id asc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    assertEquals(selectStatement.getFromTable(), "table1");
    InExpressionImpl in = (InExpressionImpl) selectStatement.getWhereClause();
    assertEquals((long) ((ConstantImpl) in.getExpressionList().get(0)).getValue(), 3L);
  }

  @Test
  public void testBetween() throws JSQLParserException {

    String sql = "select * from table1 where id between 1 and 5 order by id asc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    assertEquals(selectStatement.getFromTable(), "table1");
    BinaryExpressionImpl where = (BinaryExpressionImpl) selectStatement.getWhereClause();
    assertEquals(((BinaryExpressionImpl) where.getLeftExpression()).getOperator(), BinaryExpression.Operator.greaterEqual);
    assertEquals(((BinaryExpressionImpl) where.getRightExpression()).getOperator(), BinaryExpression.Operator.lessEqual);
    assertEquals(where.getOperator(), BinaryExpression.Operator.and);
  }

  @Test
  public void testCount() throws JSQLParserException {

    String sql = "select count(*) from table1 where id = 5";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    assertEquals(selectStatement.getFromTable(), "table1");
    assertEquals(selectStatement.getSelectColumns().get(0).getFunction(), "count");
  }

  @Test
  public void testInnerJoin() throws JSQLParserException {

    SelectStatementHandler handler = new SelectStatementHandler(client);
    String sql = "select table1.field1 as f1, table2.field1 as f2 from table1 " +
        " inner join table2 on table1.field1 = table2.field1 where table1.field1 > 0 order by table1.field1 desc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    ResultSetImpl ret = (ResultSetImpl) handler.execute("test", new ParameterHandler(), sql, statement,
        null, 1000L, 1000L, (short) 1000, false,
        null, 0);
//    assertTrue(ret.next());
//    assertEquals((long) ret.getLong("field1"), 1100L);
    for (int i = records.length - 1; i >= 0; i--) {
      ret.next();
      assertEquals((long) ret.getLong("f1"), 200 + i * 100);
      assertEquals((long) ret.getLong("f2"), 200 + i * 100);
      System.out.println(ret.getLong("f1") + ", " + ret.getLong("f2"));
    }
    //assertEquals((long)ret.getLong("field1"), 1100L);
  }

  @Test
  public void testLeftOuter() throws JSQLParserException {
    SelectStatementHandler handler = new SelectStatementHandler(client);
    String sql = "select table1.field1 as f1, table2.field1 as f2 from table1 " +
        " left outer join table2 on table1.field1 = table2.field1 where table1.field1 > 0 order by table1.field1 desc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    ResultSetImpl ret = (ResultSetImpl) handler.execute("test", new ParameterHandler(), sql, statement,
        null, 1000L, 1000L, (short) 1000, false,
        null, 0);
    for (int i = records.length - 1; i >= 0; i--) {
      ret.next();
      assertEquals((long) ret.getLong("f1"), 200 + i * 100);
      assertEquals((long) ret.getLong("f2"), 200 + i * 100);
      System.out.println(ret.getLong("f1") + ", " + ret.getLong("f2"));
    }
  }

  @Test
  public void testRightOuter() throws JSQLParserException {
    SelectStatementHandler handler = new SelectStatementHandler(client);
    String sql = "select table1.field1 as f1, table2.field1 as f2 from table1 " +
        " right outer join table2 on table1.field1 = table2.field1 where table1.field1 > 0 order by table1.field1 desc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    ResultSetImpl ret = (ResultSetImpl) handler.execute("test", new ParameterHandler(), sql, statement,
        null, 1000L, 1000L, (short) 1000, false,
        null, 0);
    for (int i = records.length - 1; i >= 0; i--) {
      ret.next();
      assertEquals((long) ret.getLong("f1"), 200 + i * 100);
      assertEquals((long) ret.getLong("f2"), 200 + i * 100);
      System.out.println(ret.getLong("f1") + ", " + ret.getLong("f2"));
    }
  }

  @Test
  public void testFull() throws JSQLParserException {
    SelectStatementHandler handler = new SelectStatementHandler(client);
    String sql = "select table1.field1 as f1, table2.field1 as f2 from table1 " +
        " full join table2 on table1.field1 = table2.field1 where table1.field1 > 0 order by table1.field1 desc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    ResultSetImpl ret = (ResultSetImpl) handler.execute("test", new ParameterHandler(), sql, statement,
        null, 1000L, 1000L, (short) 1000, false,
        null, 0);
    for (int i = records.length - 1; i >= 0; i--) {
      ret.next();
      assertEquals((long) ret.getLong("f1"), 200 + i * 100);
      assertEquals((long) ret.getLong("f2"), 200 + i * 100);
      System.out.println(ret.getLong("f1") + ", " + ret.getLong("f2"));
    }
  }

  @Test
  public void testAnd() throws JSQLParserException {
    SelectStatementHandler handler = new SelectStatementHandler(client);
    String sql = "select * from table1 where field1 > 0 and field2 < 10 order by field1 desc";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    ResultSetImpl ret = (ResultSetImpl) handler.execute("test", new ParameterHandler(), sql, statement,
        null, 1000L, 1000L, (short) 1000, false,
        null, 0);
    for (int i = records.length - 1; i >= 0; i--) {
      ret.next();
      assertEquals((long) ret.getLong("field1"), 200 + i * 100);
      System.out.println(ret.getLong("field1"));
    }
  }

  @Test
  public void testServerSelect() throws Exception {
    String sql = "select distinct field2 from table2 where field2 = '0-value'";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));

    Select selectNode = (Select) statement;

    SelectBody selectBody = selectNode.getSelectBody();
    SelectStatementImpl selectStatement = SelectStatementHandler.parseSelectStatement(client, new ParameterHandler(), (PlainSelect) selectBody, new AtomicInteger(0));
    selectStatement.setTableNames(new String[]{"table2"});
    ComObject cobj = new ComObject();
    cobj.put(ComObject.Tag.legacySelectStatement, selectStatement.serialize());
    cobj.put(ComObject.Tag.schemaVersion, client.getCommon().getSchemaVersion());
    cobj.put(ComObject.Tag.count, 100);
    cobj.put(ComObject.Tag.dbName, "test");
    cobj.put(ComObject.Tag.currOffset, 0L);
    cobj.put(ComObject.Tag.countReturned, 0L);

    Set<SelectStatementImpl.DistinctRecord> uniqueRecords = new HashSet<SelectStatementImpl.DistinctRecord>();
    ExpressionImpl.NextReturn ids = selectStatement.serverSelect("test", false, new String[]{"table2"}, false, null);
    selectStatement.applyDistinct("test", new String[]{"table2"}, ids, uniqueRecords);
    ResultSet ret = new ResultSetImpl("test", sql, client, selectStatement, new ParameterHandler(), uniqueRecords,
        new SelectContextImpl(ids, false, new String[]{"table2"}, 0, null,
            selectStatement, new ExpressionImpl.RecordCache(), false, null), null, null,
        null, null, null, null, new AtomicLong(), new AtomicLong(), null, null, false, null);

    for (int i = 0; i < 2; i++) {
      ret.next();
      assertEquals(ret.getString("field2"), (i % 2) + "-value");
      System.out.println(ret.getString("field2"));
    }
    assertFalse(ret.next());
  }
}