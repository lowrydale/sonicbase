/* © 2017 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.misc;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.index.Index;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.giraph.utils.Varint;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class TestDeleteMap {

  static class Key {
    private Object[] key;

    public Key(Object[] key) {
      this.key = key;
    }

    public int hashCode() {
      return new Long((long)key[0]).hashCode();
    }

    public boolean equals(Object obj) {

      return (long)((Key)obj).key[0] == (long)key[0];
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    File file = new File(System.getProperty("user.home"), "tmp/data.bin");

    HashMap<Key, Long> index = new HashMap<>(30_000_000);
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    long lastTime = 0;
    long lastId = 0;
    for (int i = 0; i < 50_000_000; i++) {
      long currTime = System.currentTimeMillis();

      Varint.writeUnsignedVarInt(100, out);//tablid
      Varint.writeUnsignedVarInt(100, out);//indexid
      Varint.writeUnsignedVarInt(1, out);//keylen
      out.writeBoolean(true);
      Varint.writeSignedVarLong(20_000_000, out);//key

      Varint.writeSignedVarLong(currTime - lastTime, out);
      lastTime = currTime;
      //System.out.println(Varint.sizeOfUnsignedVarInt(250_000));

      Varint.writeUnsignedVarInt(250_000, out);
      long currId = i + 2;
      //System.out.println(Varint.sizeOfUnsignedVarLong(currId - lastId));
      Varint.writeUnsignedVarLong(currId - lastId, out);
      lastId = currId;
      //index.put(new Key(new Object[]{(long)i}), (long)i + 50);

      if (i % 100000 == 0) {
        System.out.println("progress: count=" + i);
      }
    }
    out.close();
    long begin = System.currentTimeMillis();
    int i = 0;
    try {
      DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
      while (true) {
        Varint.readUnsignedVarInt(in);
        Varint.readUnsignedVarInt(in);
        Varint.readUnsignedVarInt(in);
        in.readBoolean();
        Varint.readSignedVarLong(in);

        Varint.readSignedVarLong(in);
        Varint.readUnsignedVarInt(in);
        Varint.readUnsignedVarLong(in);
        if (i++ % 100000 == 0) {
          System.out.println("progress: count=" + i);
        }
      }
    }
    catch (EOFException e) {
      //ok
    }
    System.out.println("duration=" + (System.currentTimeMillis() - begin));

  }
}
