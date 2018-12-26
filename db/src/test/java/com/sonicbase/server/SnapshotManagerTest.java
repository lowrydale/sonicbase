package com.sonicbase.server;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.FileUtils;
import com.sonicbase.common.ServersConfig;
import com.sonicbase.index.AddressMap;
import com.sonicbase.index.Index;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SnapshotManagerTest {

  private DatabaseServer server;

  @BeforeClass
  public void beforeClass() {
    System.setProperty("log4j.configuration", "test-log4j.xml");

    server = mock(DatabaseServer.class);

    Map<String, DatabaseServer.SimpleStats> stats = DatabaseServer.initStats();
    when(server.getStats()).thenReturn(stats);
  }

  @Test
  public void test() throws Exception {
    AddressMap addressMap = new AddressMap(server);
    when(server.getAddressMap()).thenReturn(addressMap);
    when(server.getBatchRepartCount()).thenReturn(new AtomicInteger(0));
    Map<Integer, TableSchema> tables = new HashMap<>();
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);

    when(server.getDataDir()).thenReturn("/tmp/database");
    FileUtils.deleteDirectory(new File("/tmp/database"));

    when(server.getIndexSchema(anyString(), anyString(), anyString())).thenReturn(indexSchema);

    DatabaseCommon common = TestUtils.createCommon(tableSchema);
    String configStr = IOUtils.toString(BulkImportManager.class.getResourceAsStream("/config/config-1-local.yaml"), "utf-8");
    Config config = new Config(configStr);
    ServersConfig serversConfig = new ServersConfig("test", config.getShards(), true, true);
    common.setServersConfig(serversConfig);
    when(server.getCommon()).thenReturn(common);
    when(server.getConfig()).thenReturn(config);
    when(server.getSchemaManager()).thenReturn(mock(SchemaManager.class));
    when(server.getLogManager()).thenReturn(mock(LogManager.class));

    Index index = new Index(tableSchema, indexSchema.getName(), indexSchema.getComparators());
    when(server.getIndex(anyString(), anyString(), anyString())).thenReturn(index);


    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<Object[]> keys = TestUtils.createKeys(10);

    int k = 0;
    for (Object[] key : keys) {
      Object address = addressMap.toUnsafeFromRecords(new byte[][]{records[k]});
      index.put(key, address);
      k++;
    }

    DatabaseClient client = mock(DatabaseClient.class);
    when(client.getCommon()).thenReturn(common);
    when(server.getClient()).thenReturn(client);
    when(server.getUpdateManager()).thenReturn(mock(UpdateManager.class));

    SnapshotManager snapshotManager = new SnapshotManager(server);

    snapshotManager.runSnapshot("test");

    index.clear();

    snapshotManager.recoverFromSnapshot("test");

    for (int j = 0; j < keys.size(); j++) {
      assertTrue(index.get(keys.get(j)) != null);
    }
    assertEquals(index.size(), records.length);

  }

  @Test
  public void testDeleteIndexSchema() throws IOException {
    when(server.getDataDir()).thenReturn("/tmp/database");
    FileUtils.deleteDirectory(new File("/tmp/database"));

    SnapshotManager snapshot = new SnapshotManager(server);
    File file = new File(snapshot.getSnapshotSchemaDir("test"),  "table1/indices/index1/file");
    file.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      writer.write("test");
    }
    assertTrue(file.exists());

    snapshot.deleteIndexSchema("test", 100, "table1", "index1");
    assertFalse(file.exists());

  }

  @Test
  public void testDeleteTableSchema() throws IOException {
    when(server.getDataDir()).thenReturn("/tmp/database");
    FileUtils.deleteDirectory(new File("/tmp/database"));

    SnapshotManager snapshot = new SnapshotManager(server);
    File file = new File(snapshot.getSnapshotSchemaDir("test"),  "table1/file");
    file.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      writer.write("test");
    }
    assertTrue(file.exists());

    snapshot.deleteTableSchema("test", 100, "table1");
    assertFalse(file.exists());
  }

  @Test
  public void testDeleteDbSchema() throws IOException {
    when(server.getDataDir()).thenReturn("/tmp/database");
    FileUtils.deleteDirectory(new File("/tmp/database"));

    SnapshotManager snapshot = new SnapshotManager(server);
    File file = new File(snapshot.getSnapshotSchemaDir("test"),  "table1/file");
    file.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))) {
      writer.write("test");
    }
    assertTrue(file.exists());

    snapshot.deleteDbSchema("test");
    assertFalse(file.exists());

  }

}
