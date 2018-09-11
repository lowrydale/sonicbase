/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.client.SelectStatementHandler;
import com.sonicbase.common.*;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ClientTestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;

public class ExpressionImplTest {

  @Test
  public void testCounter() throws JSQLParserException, IOException {
    String sql = "select count(*) from persons";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));
    DatabaseCommon common = new DatabaseCommon();
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(1);
    when(client.getReplicaCount()).thenReturn(1);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      when(client.send(eq("ReadManager:countRecords"), anyInt(), anyLong(), anyObject(), anyObject())).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          ComObject retObj = new ComObject();
          retObj.put(ComObject.Tag.COUNT_LONG, 2_001L);
          return retObj.serialize();
        }
      });
      when(client.getExecutor()).thenReturn(executor);
      SelectStatementHandler handler = new SelectStatementHandler(client);
      Select selectNode = (Select) statement;
      SelectBody selectBody = selectNode.getSelectBody();

      SelectStatementImpl selectStatement = handler.parseSelectStatement(client, null,
          (PlainSelect) selectBody, new AtomicInteger());

      ResultSetImpl ret = (ResultSetImpl) selectStatement.execute("test", sql, null, 0L, 0L, (short) 0, false, null, 0);
      ret.next();
      assertEquals((long)ret.getLong(1), 2_001L);
    }
    finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testMax() throws JSQLParserException, IOException {
    String sql = "select max(field1) from table1";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));
    DatabaseCommon common = new DatabaseCommon();
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    final byte[][] records = ClientTestUtils.createRecords(common, tableSchema, 10);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(1);
    when(client.getReplicaCount()).thenReturn(1);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      when(client.send(eq("ReadManager:indexLookup"), anyInt(), anyLong(), anyObject(), anyObject())).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
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
        }
      });
      when(client.getExecutor()).thenReturn(executor);
      SelectStatementHandler handler = new SelectStatementHandler(client);
      Select selectNode = (Select) statement;
      SelectBody selectBody = selectNode.getSelectBody();

      SelectStatementImpl selectStatement = handler.parseSelectStatement(client, null,
          (PlainSelect) selectBody, new AtomicInteger());

      ResultSetImpl ret = (ResultSetImpl) selectStatement.execute("test", sql, null, 0L, 0L, (short) 0, false, null, 0);
      ret.next();
      assertEquals(200, (long)ret.getLong(1));
    }
    finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void testAvg() throws JSQLParserException, IOException {
    String sql = "select avg(field1) as avgValue from table1";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));
    DatabaseCommon common = new DatabaseCommon();
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    final byte[][] records = ClientTestUtils.createRecords(common, tableSchema, 10);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);
    JsonNode node = new ObjectMapper().readTree(" { \"shards\" : [\n" +
        "    {\n" +
        "      \"replicas\": [\n" +
        "        {\n" +
        "          \"publicAddress\": \"localhost\",\n" +
        "          \"privateAddress\": \"localhost\",\n" +
        "          \"port\": 9010,\n" +
        "          \"httpPort\": 8080\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]}\n");
    ServersConfig serversConfig = new ServersConfig("test", (ArrayNode) ((ObjectNode)node).withArray("shards"), true, true);
    //when(common.getServersConfig()).thenReturn(serversConfig);
    common.setServersConfig(serversConfig);
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(client.getShardCount()).thenReturn(2);
    when(client.getReplicaCount()).thenReturn(1);
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {

      final AtomicInteger offset = new AtomicInteger();
      when(client.send(eq("ReadManager:evaluateCounterWithRecord"), anyInt(), anyLong(), anyObject(), anyObject())).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          ComObject retObj = new ComObject();
          Counter counter = new Counter();
          counter.setDataType(DataType.Type.BIGINT);
          counter.setTableName(tableSchema.getName());
          counter.setColumn(1);
          counter.setColumnName("field1");
          counter.setDestTypeToLong();
          if (offset.get() == 0) {
            counter.setMinLong(9223372036854775807L);
            counter.setMaxLong(0L);
          }
          else if (offset.get() == 1) {
            counter.setMinLong(9223372036854775807L);
            counter.setMaxLong(8L);
          }
          else if (offset.get() == 2) {
            counter.setMinLong(9223372036854775807L);
            counter.setMaxLong(9L);
          }
          else if (offset.get() == 3) {
            counter.setMinLong(9223372036854775807L);
            counter.setMaxLong(109L);
          }
          offset.incrementAndGet();
          counter.setCount(0L);
          retObj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());
          return retObj.serialize();
        }
      });
      when(client.send(eq("ReadManager:evaluateCounterGetKeys"), anyInt(), anyLong(), anyObject(), anyObject())).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          ComObject retObj = new ComObject();
          byte[] minKey = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), new Object[]{0L});
          if (minKey != null) {
            retObj.put(ComObject.Tag.MIN_KEY, minKey);
          }
          byte[] maxKey = DatabaseCommon.serializeKey(tableSchema, indexSchema.getName(), new Object[]{200L});
          if (maxKey != null) {
            retObj.put(ComObject.Tag.MAX_KEY, maxKey);
          }
          Counter counter = new Counter();
          counter.setDataType(DataType.Type.BIGINT);
          counter.setTableName(tableSchema.getName());
          counter.setColumn(1);
          counter.setColumnName("field1");
          counter.setDestTypeToLong();
          counter.setMaxLong(-9223372036854775808L);
          counter.setMinLong(9223372036854775807L);
          counter.setCount(0L);
          retObj.put(ComObject.Tag.LEGACY_COUNTER, counter.serialize());

          return retObj.serialize();
        }
      });
      when(client.send(eq("ReadManager:indexLookup"), anyInt(), anyLong(), anyObject(), anyObject())).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
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

          array = retObj.putArray(ComObject.Tag.COUNTERS, ComObject.Type.BYTE_ARRAY_TYPE);

          Counter counter = new Counter();
          counter.setDataType(DataType.Type.BIGINT);
          counter.setTableName(tableSchema.getName());
          counter.setColumn(1);
          counter.setColumnName("field1");
          counter.setDestTypeToLong();
          counter.setMaxLong(109L);
          counter.setMinLong(0L);
          counter.setCount(20L);
          counter.setLongCount(1095L);

          array.add(counter.serialize());


          return retObj.serialize();
        }
      });
      when(client.getExecutor()).thenReturn(executor);
      SelectStatementHandler handler = new SelectStatementHandler(client);
      Select selectNode = (Select) statement;
      SelectBody selectBody = selectNode.getSelectBody();

      SelectStatementImpl selectStatement = handler.parseSelectStatement(client, null,
          (PlainSelect) selectBody, new AtomicInteger());

      ResultSetImpl ret = (ResultSetImpl) selectStatement.execute("test", null, null, 0L, 0L, (short) 0, false, null, 0);
      ret.next();
      assertEquals(54.75d, (double)ret.getDouble("avgValue"));
    }
    finally {
      executor.shutdownNow();
    }
  }
}
