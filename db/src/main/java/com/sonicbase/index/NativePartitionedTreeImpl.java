package com.sonicbase.index;

import com.sonicbase.common.FileUtils;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupOneKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativePartitionedTreeImpl extends NativePartitionedTree implements IndexImpl {
  private static final Logger logger = LoggerFactory.getLogger(NativePartitionedTreeImpl.class);

  private final int port;

  public static boolean isWindows() {
    return !OS.contains("cygwin") && OS.contains("win");
  }

  public static boolean isCygwin() {
    return OS.contains("cygwin");
  }

  public static boolean isMac() {
    return OS.contains("mac");
  }

  public static boolean isUnix() {
    return OS.contains("nux");
  }

  private static String OS = System.getProperty("os.name").toLowerCase();

  private final long indexId;
  private final Index index;
  private static boolean initializedNative = false;
  private static Object mutex = new Object();

  public NativePartitionedTreeImpl(int port, Index index) {

    init(port);

    this.port = port;
    this.index = index;

    TableSchema tableSchema = index.getTableSchema();
    IndexSchema schema = index.getIndexSchema();
    String[] fields = schema.getFields();
    int[] dataTypes = new int[fields.length];
    for (int i = 0; i < fields.length; i++) {
      int offset = tableSchema.getFieldOffset(fields[i]);
      FieldSchema field = tableSchema.getFields().get(offset);
      dataTypes[i] = field.getType().getValue();
    }

    this.indexId = initIndex(dataTypes);
  }

  public static void init(int port) {
    try {
      synchronized (mutex) {
        if (!initializedNative) {

          //System.load("/home/ec2-user/PartitionedAvlTree/linux/SonicBase.so");

          String libName = "";
          String name = ManagementFactory.getRuntimeMXBean().getName();
          String pid = name.split("@")[0];

          if (isWindows()) {
            libName = "/win/SonicBase.dll";
          } else if (isCygwin()) {
            libName = "/win/SonicBase.dll";
          } else if (isMac()) {
            libName = "/mac/SonicBase.so";
          } else if (isUnix()) {
            libName = "/linux/SonicBase.so";
          }
          URL url = NativePartitionedTreeImpl.class.getResource(libName);
          String tmpdir = System.getProperty("java.io.tmpdir");
          File tmpDir = new File(new File(tmpdir), "SonicBase-Native-" + port);
          File nativeLibTmpFile = new File(tmpDir, libName);
          FileUtils.deleteDirectory(nativeLibTmpFile.getParentFile());
          nativeLibTmpFile.getParentFile().mkdirs();
          try (InputStream in = url.openStream()) {
            Files.copy(in, nativeLibTmpFile.toPath());
          }
          System.load(nativeLibTmpFile.getAbsolutePath());

          initializedNative = true;
        }
      }
    }
    catch (Exception e) {
      logger.error("Unable to load native library", e);
      throw new DatabaseException(e);
    }
  }

  @Override
  public void logError(String msg) {
    logger.error(msg);
  }


  @Override
  public Object put(Object[] key, Object value) {
    long ret = put(indexId, key, (long)value);
    if (ret == -1) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  @Override
  public Object get(Object[] key) {
    long ret = get(indexId, key);
    if (-1 == ret) {
      return null;
    }
    return ret;
  }

  @Override
  public Object remove(Object[] key) {
    long ret = remove(indexId, key);
    if (ret != -1) {
      index.getSizeObj().decrementAndGet();
      return ret;
    }
    return null;
  }

  public void sortKeys(Object[][] keys, boolean ascend) {
    sortKeys(indexId, keys, ascend);
  }

  @Override
  public int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      return tailBlockArray(indexId, startKey, count, first, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      return headBlockArray(indexId, startKey, count, first, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];
    boolean found = higherEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];
    boolean found = lowerEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = floorEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public List<Map.Entry<Object[], Object>> equalsEntries(Object[] key) {
    List<Map.Entry<Object[], Object>> ret = new ArrayList<>();

    synchronized (this) {
      Map.Entry<Object[], Object> entry = floorEntry(key);
      if (entry == null) {
        return null;
      }
      ret.add(entry);
      while (true) {
        entry = higherEntry(entry.getKey());
        if (entry == null) {
          break;
        }
        if (0 != index.getComparator().compare(entry.getKey(), key)) {
          return ret;
        }
        ret.add(entry);
      }
      return ret;
    }
  }


  @Override
  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = ceilingEntry(indexId, key, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> lastEntry() {

    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = lastEntry2(indexId, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public Map.Entry<Object[], Object> firstEntry() {

    Object[][] keys = new Object[1][];
    long[] values = new long[1];

    boolean found = firstEntry2(indexId, keys, values);
    if (!found) {
      return null;
    }
    return new Index.MyEntry(keys[0], values[0]);
  }

  @Override
  public void clear() {
    clear(indexId);
  }

  @Override
  public void delete() {

  }

}
