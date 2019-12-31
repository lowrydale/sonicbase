package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.query.ResultSet;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ClientTestUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.sonicbase.client.DatabaseClient.SERIALIZATION_VERSION;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class DescribeStatementHandlerTest {

  @Test
  public void test() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe table table1");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "-----------------------------------\n" +
        "| Name    | Type          | Width |\n" +
        "-----------------------------------\n" +
        "| _id     | BIGINT        | 0     |\n" +
        "| field1  | BIGINT        | 0     |\n" +
        "| field2  | VARCHAR       | 0     |\n" +
        "| field3  | TIMESTAMP     | 0     |\n" +
        "| field4  | INTEGER       | 0     |\n" +
        "| field5  | SMALLINT      | 0     |\n" +
        "| field6  | TINYINT       | 0     |\n" +
        "| field7  | CHAR          | 0     |\n" +
        "| field8  | NCHAR         | 0     |\n" +
        "| field9  | FLOAT         | 0     |\n" +
        "| field10 | REAL          | 0     |\n" +
        "| field11 | DOUBLE        | 0     |\n" +
        "| field12 | BOOLEAN       | 0     |\n" +
        "| field13 | BIT           | 0     |\n" +
        "| field14 | VARCHAR       | 0     |\n" +
        "| field15 | CLOB          | 0     |\n" +
        "| field16 | NCLOB         | 0     |\n" +
        "| field17 | LONGVARCHAR   | 0     |\n" +
        "| field18 | NVARCHAR      | 0     |\n" +
        "| field19 | LONGNVARCHAR  | 0     |\n" +
        "| field20 | LONGVARBINARY | 0     |\n" +
        "| field21 | VARBINARY     | 0     |\n" +
        "| field22 | BLOB          | 0     |\n" +
        "| field23 | NUMERIC       | 0     |\n" +
        "| field24 | DECIMAL       | 0     |\n" +
        "| field25 | DATE          | 0     |\n" +
        "| field26 | TIME          | 0     |\n" +
        "| field27 | TIMESTAMP     | 0     |\n" +
        "-----------------------------------\n" +
        "Index=_primarykey\n" +
        "---------------------------\n" +
        "| Name   | Type   | Width |\n" +
        "---------------------------\n" +
        "| field1 | BIGINT | 0     |\n" +
        "---------------------------\n";

    assertEquals(builder.toString(), expected);
  }


  @Test
  public void testIndex() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe index table1._primarykey");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "---------------------------\n" +
        "| Name   | Type   | Width |\n" +
        "---------------------------\n" +
        "| field1 | BIGINT | 0     |\n" +
        "---------------------------\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testTables() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe tables");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "table1\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testShards() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe shards");
    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      String line = ret.getString(1);
      builder.append(line).append("\n");
    }

    String expected = "Table=table1, Index=_primarykey, shard=0, key=[null], lastKey=[null]\n";

    assertEquals(builder.toString(), expected);
  }

  @Test
  public void testServerStats() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon() {
      public boolean haveProLicense() {
        return true;
      }
      public ServersConfig getServersConfig() {
        ServersConfig config = mock(ServersConfig.class);
        when(config.getShards()).thenAnswer(new Answer(){
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            ServersConfig.Shard[] shards = new ServersConfig.Shard[]{
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 9010, true),
                    new ServersConfig.Host("localhost", 9060, false)
                }),
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 10010, false),
                    new ServersConfig.Host("localhost", 10060, true)
                })
              };
            return shards;
          }
        });
        return config;
      }
    };
    ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 10_000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
    try {
      when(client.getExecutor()).thenReturn(executor);
      when(client.getCommon()).thenReturn(common);
      when(client.getShardCount()).thenReturn(2);
      when(client.getReplicaCount()).thenReturn(2);
      when(client.getServersArray()).thenAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          DatabaseClient.Server[][] servers = new DatabaseClient.Server[][]{
              new DatabaseClient.Server[]{
                  new DatabaseClient.Server("localhost", 9010),
                  new DatabaseClient.Server("localhost", 9060)},
              new DatabaseClient.Server[]{
                  new DatabaseClient.Server("localhost", 10010),
                  new DatabaseClient.Server("localhost", 10060)
              }};
          servers[0][0].setDead(true);
          servers[1][1].setDead(true);
          return servers;
        }
      });

      when(client.send(eq("OSStatsManager:getOSStats"), anyInt(), anyInt(), anyObject(), eq(DatabaseClient.Replica.SPECIFIED))).thenAnswer(new Answer(){
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          ComObject retObj = new ComObject(10);
          retObj.put(ComObject.Tag.RES_GIG, 40d);
          retObj.put(ComObject.Tag.CPU, 80d);
          retObj.put(ComObject.Tag.JAVA_MEM_MIN, 30d);
          retObj.put(ComObject.Tag.JAVA_MEM_MAX, 40d);
          retObj.put(ComObject.Tag.AVG_REC_RATE, 20d);
          retObj.put(ComObject.Tag.AVG_TRANS_RATE, 20d);
          retObj.put(ComObject.Tag.DISK_AVAIL, "100g");
          retObj.put(ComObject.Tag.HOST, "localhost");
          Object[] args = invocationOnMock.getArguments();
          if ((int)args[1] == 0 && (long)args[2] == 0) {
            retObj.put(ComObject.Tag.PORT, 9010);
          }
          else if ((int)args[1] == 0 && (long)args[2] == 1) {
            retObj.put(ComObject.Tag.PORT, 9060);
          }
          if ((int)args[1] == 1 && (long)args[2] == 0) {
            retObj.put(ComObject.Tag.PORT, 10010);
          }
          else if ((int)args[1] == 1 && (long)args[2] == 1) {
            retObj.put(ComObject.Tag.PORT, 10060);
          }
          return retObj;
        }
      });
      TableSchema tableSchema = ClientTestUtils.createTable();
      IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
      common.getTables("test").put(tableSchema.getName(), tableSchema);

      DescribeStatementHandler handler = new DescribeStatementHandler(client);
      ResultSet ret = handler.doDescribe("test", "describe server stats");

      for (int i = 0; i < 4; i++) {
        ret.next();

        String line = ret.getString("host");
        if (line.equals("localhost:9060") || line.equals("localhost:10010")) {
          String cpu = ret.getString("cpu");
          assertEquals(cpu, "80");
        }
        else {
          String cpu = ret.getString("cpu");
          assertEquals(cpu, "?");
        }
      }
      assertFalse(ret.next());

    }
    finally {
      executor.shutdownNow();
    }
  }
//
  @Test
  public void testSchemaVersion() {

    DatabaseCommon common = new DatabaseCommon() {
      public int getSchemaVersion() {
        return 101;
      }
      public ServersConfig getServersConfig() {
        ServersConfig config = mock(ServersConfig.class);
        when(config.getShards()).thenAnswer(new Answer(){
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            ServersConfig.Shard[] shards = new ServersConfig.Shard[]{
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 9010, false),
                    new ServersConfig.Host("localhost", 9060, false)
                }),
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 10010, false),
                    new ServersConfig.Host("localhost", 10060, false)
                })
            };
            return shards;
          }
        });
        return config;
      }
    };
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);

    when(client.send(eq("DatabaseServer:getSchema"), anyInt(), anyInt(), anyObject(), anyObject())).thenAnswer(new Answer(){
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        ComObject retObj = new ComObject(2);
        Object[] args = invocationOnMock.getArguments();
        if ((int)args[1] == 0 && (long)args[2] == 0) {
          common.setSchemaVersion(100);
        }
        else {
          common.setSchemaVersion(101);
        }
        byte[] bytes = common.serializeSchema(SERIALIZATION_VERSION);
        retObj.put(ComObject.Tag.SCHEMA_BYTES, bytes);
        return retObj;
      }
    });

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe schema version");
    for (int i = 0; i < 4; i++) {
      ret.next();
      if (ret.getString("host").equals("localhost:9010")) {
        assertEquals(ret.getString("version"), "100");
      }
      else {
        assertEquals(ret.getString("version"), "101");
      }
    }
  }

  @Test
  public void testServerHealth() {

    DatabaseCommon common = new DatabaseCommon() {
      public int getSchemaVersion() {
        return 101;
      }
      public ServersConfig getServersConfig() {
        ServersConfig config = mock(ServersConfig.class);
        when(config.getShards()).thenAnswer(new Answer(){
          @Override
          public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
            ServersConfig.Shard[] shards = new ServersConfig.Shard[]{
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 9010, true),
                    new ServersConfig.Host("localhost", 9060, false)
                }),
                new ServersConfig.Shard(new ServersConfig.Host[]{
                    new ServersConfig.Host("localhost", 10010, false),
                    new ServersConfig.Host("localhost", 10060, true)
                })
            };
            return shards;
          }
        });
        return config;
      }
    };
    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);

    DescribeStatementHandler handler = new DescribeStatementHandler(client);
    ResultSet ret = handler.doDescribe("test", "describe server health");
    for (int i = 0; i < 4; i++) {
      ret.next();
      if (ret.getString("host").equals("localhost:9010") ||
          ret.getString("host").equals("localhost:10060")) {
        assertEquals(ret.getString("dead"), "true");
      }
      else {
        assertEquals(ret.getString("dead"), "false");
      }
    }
    assertFalse(ret.next());
  }

  @Test
  public void testLicenses() {
    DatabaseClient client = mock(DatabaseClient.class);

    DescribeStatementHandler handler = new DescribeStatementHandler(client) {
      public InputStream urlGet(String url) {
        String ret = "{\"totalCores\": 8,\n" +
            "\"allocatedCores\": 32,\n" +
            "\"inCompliance\": true,\n" +
            "\"disableNow\": false,\n" +
            "\"disableDate\": \"08-19-18\",\n" +
            "\"multipleLicenseServers\": false}";
        try {
          return new ByteArrayInputStream(ret.getBytes("utf-8"));
        }
        catch (UnsupportedEncodingException e) {
          throw new DatabaseException(e);
        }
      }

      public String getLicenseServerConfig() {
        return "{\n" +
            "  \"installDirectory\": \"$HOME/sonicbase\",\n" +
            "  \"user\": \"ec2-user\",\n" +
            "  \"server\": {\n" +
            "    \"address\": \"localhost\",\n" +
            "    \"port\": 8443\n" +
            "  }\n" +
            "}";
      }
    };
    ResultSet ret = handler.doDescribe("test", "describe licenses");

    StringBuilder builder = new StringBuilder();
    while (ret.next()) {
      builder.append(ret.getString(1) + "\n");
    }
    assertEquals(builder.toString(), "total cores in use=8\n" +
        "total allocated cores=32\n" +
        "in compliance=true\n" +
        "disabling now=false\n" +
        "disabling date=08-19-18\n" +
        "multiple license servers=false\n");
  }

}
