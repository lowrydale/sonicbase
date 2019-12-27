/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.bench;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Config;
import com.sonicbase.common.DataUtils;
import com.sonicbase.index.Index;
import com.sonicbase.index.NativePartitionedTreeImpl;
import com.sonicbase.jdbcdriver.ConnectionProxy;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.Schema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.DatabaseServer;
import org.anarres.lzo.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.sql.DriverManager.*;

public class SimpleJniBenchmark {

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ExecutionException, InterruptedException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;
    LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(algorithm, null);
    LzoOutputStream outStream = new LzoOutputStream(out, compressor);

    byte[] bytes2 = new byte[100];
    int[] offset = new int[]{0};
    DataUtils.writeSignedVarLong(1909090909090909090L, bytes2, offset);
    outStream.write(bytes2, 0, offset[0]);
    outStream.close();

    ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
    LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(algorithm, null);
    LzoInputStream stream = new LzoInputStream(in, decompressor);
    byte[] bytes1 = IOUtils.toByteArray(stream);


    byte[] bytes = new byte[200];
    DataUtils.writeSignedVarLong(190909090909090909L, bytes, offset);
    System.out.println(offset[0]);
    offset[0] = 0;
    long v = DataUtils.readSignedVarLong(bytes1, offset);
    System.out.println(v);



    String configStr = IOUtils.toString(new BufferedInputStream(SimpleJniBenchmark.class.getResourceAsStream("/config/config-4-servers.yaml")), "utf-8");
    Config config = new Config(configStr);

    FileUtils.deleteDirectory(new File(System.getProperty("user.home"), "db-data"));


    DatabaseClient.getServers().clear();

    String role = "primaryMaster";

    final DatabaseServer[] dbServers = new DatabaseServer[4];

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < dbServers.length; i++) {
      //      futures.add(executor.submit(new Callable() {
      //        @Override
      //        public Object call() throws Exception {
      //          String role = "primaryMaster";

      dbServers[i] = new DatabaseServer();
      Config.copyConfig("4-servers");
      dbServers[i].setConfig(config, "localhost", 9010 + (50 * i), true, new AtomicBoolean(true), new AtomicBoolean(true), null, false);
      dbServers[i].setRole(role);
      //          return null;
      //        }
      //      }));
    }
    for (Future future : futures) {
      future.get();
    }

    for (DatabaseServer server : dbServers) {
      server.shutdownRepartitioner();
    }
    Class.forName("com.sonicbase.jdbcdriver.Driver");

    Connection conn = getConnection("jdbc:sonicbase:localhost:9010", "user", "password");

    ((ConnectionProxy) conn).getDatabaseClient().createDatabase("test");

    conn.close();

    conn = getConnection("jdbc:sonicbase:localhost:9010/test", "user", "password");

    PreparedStatement stmt = conn.prepareStatement("create table Persons (id BIGINT, id2 BIGINT, socialSecurityNumber VARCHAR(20), relatives VARCHAR(64000), restricted BOOLEAN, gender VARCHAR(8), PRIMARY KEY (id))");
    stmt.executeUpdate();

    stmt = conn.prepareStatement("create table Memberships (personId BIGINT, personId2 BIGINT, membershipName VARCHAR(20), resortId BIGINT, PRIMARY KEY (personId, membershipName))");
    stmt.executeUpdate();


    DatabaseClient client = ((ConnectionProxy) conn).getDatabaseClient();



    Schema schema = client.getSchema("test");
    TableSchema tableSchema = schema.getTables().get("memberships");
    IndexSchema indexSchema = tableSchema.getIndices().get("_primarykey");
    Index index = new Index(9010, new HashMap<Long, Boolean>(), tableSchema, indexSchema.getName(), indexSchema.getComparators());
    NativePartitionedTreeImpl tree = new NativePartitionedTreeImpl(8080, index);

    long begin = System.currentTimeMillis();
    AtomicLong count = new AtomicLong();
    Object[][] keys = new Object[1000][];
    long[] values = new long[1000];
    for (int i = 0; i < 2_000_000; i++) {
      tree.getResultsObjs(new Object[]{1}, 1000, true, keys, values);
      //tree.getResultsByteArray(new Object[]{1}, 1000, true);
      if (count.incrementAndGet() % 1_000 == 0) {
        System.out.println("progress: count=" + count.get() * 1000 + ", rate=" + ((double)count.get() * 1000 / (System.currentTimeMillis() - begin) * 1000f));
      }
    }
  }
}
