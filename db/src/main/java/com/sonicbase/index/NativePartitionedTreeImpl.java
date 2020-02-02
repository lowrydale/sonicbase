package com.sonicbase.index;

import com.sonicbase.common.DataUtils;
import com.sonicbase.common.FileUtils;
import com.sonicbase.jdbcdriver.Parameter;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.schema.FieldSchema;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import org.anarres.lzo.LzoDecompressor1x;
import org.anarres.lzo.lzo_uintp;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.sql.Types.*;

public class NativePartitionedTreeImpl extends NativePartitionedTree implements IndexImpl {
  private static final Logger logger = LoggerFactory.getLogger(NativePartitionedTreeImpl.class);

  private final int port;
  private final int[] dataTypes;

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
    this.dataTypes = dataTypes;
  }

  public static void init(int port) {
    try {
      synchronized (mutex) {
        if (!initializedNative) {

          //System.load("/home/ec2-user/PartitionedAvlTree/linux/SonicBase.so");

          String libName = "";
          String name = ManagementFactory.getRuntimeMXBean().getName();
          String pid = name.split("@")[0];

          String libName2 = "";
          if (isWindows()) {
            libName = "/win/SonicBase.dll";
            libName2 = "\\win\\SonicBase.dll";
          } else if (isCygwin()) {
            libName = "/win/SonicBase.dll";
            libName2 = "\\win\\SonicBase.dll";
          } else if (isMac()) {
            libName = "/mac/SonicBase.so";
            libName2 = "/mac/SonicBase.so";
          } else if (isUnix()) {
            libName = "/linux/SonicBase.so";
            libName2 = "/linux/SonicBase.so";
          }
          URL url = NativePartitionedTreeImpl.class.getResource(libName);
          String tmpdir = System.getProperty("java.io.tmpdir");
          File tmpDir = new File(new File(tmpdir), "SonicBase-Native-" + port);
          File nativeLibTmpFile = new File(tmpDir, libName2);
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

  public void put(Object[][] keys,  long[] values, long[] retValues) {
    put(indexId, keys, values, retValues);
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


  public static Object[] deserializeKey(int[] dataTypes, byte[] bytes, int[] offset) {

    Object[] ret = new Object[dataTypes.length];
    for (int i = 0; i < ret.length; i++) {
      byte hasValue = bytes[offset[0]++];
      if (hasValue == 0) {
        continue;
      }
      switch (dataTypes[i]) {
        case ROWID:
        case BIGINT: {
          long v = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          ret[i] = v;
        }
          break;
        case INTEGER: {
          int v = (int) DataUtils.bytesToInt(bytes, offset[0]);
          offset[0] += 4;
          ret[i] = v;
        }
          break;
        case SMALLINT: {
          short v = (short) DataUtils.bytesToShort(bytes, offset[0]);
          offset[0] += 2;
          ret[i] = v;
        }
          break;
        case TINYINT: {
          ret[i] = bytes[offset[0]++];
        }
          break;
        case DATE: {
          long v = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          Date d = new Date(v);
          ret[i] = d;
        }
          break;
        case TIME: {
          long v = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          Time t = new Time(0);
          t.setTime(v);
          ret[i] = t;
        }
          break;
        case TIMESTAMP: {
          long v1 = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          long v2 = (int) DataUtils.bytesToInt(bytes, offset[0]);
          offset[0] += 4;
          Timestamp t = new Timestamp(0);
          t.setTime(v1);
          t.setNanos((int) v2);
          ret[i] = t;
        }
          break;
        case DOUBLE:
        case FLOAT: {
          long value = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          ret[i] = Double.longBitsToDouble(value);
        }
          break;
        case REAL: {
          int value = DataUtils.bytesToInt(bytes, offset[0]);
          offset[0] += 4;
          ret[i] = Float.intBitsToFloat(value);
        }
          break;
        case NUMERIC:
        case DECIMAL: {
          long len = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          char[] chars = new char[(int) len];
          for (int j = 0; j < len; j++) {
            byte b1 = bytes[offset[0]++];
            byte b2 = bytes[offset[0]++];
            char c = (char) ((b1 & 0xFF) << 8 | (b2 & 0xFF));
            chars[j] = c;
          }
          BigDecimal b = new BigDecimal(new String(chars));
          ret[i] = b;
        }
          break;
        case VARCHAR:
        case CHAR:
        case LONGVARCHAR:
        case NCHAR:
        case NVARCHAR:
        case LONGNVARCHAR:
        case NCLOB: {
          long len = DataUtils.bytesToLong(bytes, offset[0]);
          offset[0] += 8;
          char[] chars = new char[(int) len];
          for (int j = 0; j < len; j++) {
            byte b1 = bytes[offset[0]++];
            byte b2 = bytes[offset[0]++];
            char c = (char) ((b1 & 0xFF) << 8 | (b2 & 0xFF));
            chars[j] = c;
          }
          ret[i] = chars;
        }
          break;
        default:
        throw new DatabaseException("unsupported type: type=" + dataTypes[i]);
      }
    }
    return ret;
  }


  private ConcurrentLinkedQueue<byte[]> bytePool = new ConcurrentLinkedQueue<>();

  @Override
  public int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    int retCount = 0;
    try {
      byte[] bytes = bytePool.poll();
      if (bytes == null) {
        bytes = new byte[218196];
      }

      while (!tailBlockArray(indexId, startKey, count, first, bytes, bytes.length)) {
        bytes = new byte[bytes.length * 2];
      }

      /*
      //int len = DataUtils.bytesToInt(bytes, 0);
      LzoDecompressor1x d = new LzoDecompressor1x();

      lzo_uintp retLen = new lzo_uintp();
      byte[] out = bytePool.poll();
      if (out == null) {
        out = new byte[8196];
      }
      d.decompress(bytes, 0, bytes.length, out, 0, retLen);

      if (bytePool.size() < 100) {
        bytePool.offer(out);
      }

      bytes = out;
*/
      int[] offset = new int[]{0};
      retCount = (int) DataUtils.bytesToLong(bytes, offset[0]);
      offset[0] += 8;
//
      for (int i = 0; i < retCount; i++) {
        keys[i] = deserializeKey(dataTypes, bytes, offset);
        values[i] = DataUtils.bytesToLong(bytes, offset[0]);
        offset[0] += 8;
      }

      if (bytePool.size() < 100) {
        bytePool.offer(bytes);
      }

      return retCount;
    }
    catch (Exception e) {
      throw new RuntimeException("retCount=" + retCount, e);
      //return 0;
    }
  }

  public int getResultsObjs(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      return getResultsObjects(indexId, startKey, count, first, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getResultsByteArray(Object[] startKey, int count, boolean first) {
    try {
      return getResultsBytes(indexId, startKey, count, first);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    int retCount = 0;
    try {
      byte[] bytes = bytePool.poll();
      if (bytes == null) {
        bytes = new byte[218196];
      }

      while (!headBlockArray(indexId, startKey, count, first, bytes, bytes.length)) {
        bytes = new byte[bytes.length * 2];
      }

      /*
      //int len = DataUtils.bytesToInt(bytes, 0);
      LzoDecompressor1x d = new LzoDecompressor1x();

      lzo_uintp retLen = new lzo_uintp();
      byte[] out = bytePool.poll();
      if (out == null) {
        out = new byte[8196];
      }
      d.decompress(bytes, 0, bytes.length, out, 0, retLen);

      if (bytePool.size() < 100) {
        bytePool.offer(out);
      }

      bytes = out;
*/
      int[] offset = new int[]{0};
      retCount = (int) DataUtils.bytesToLong(bytes, offset[0]);
      offset[0] += 8;
//
      for (int i = 0; i < retCount; i++) {
        keys[i] = deserializeKey(dataTypes, bytes, offset);
        values[i] = DataUtils.bytesToLong(bytes, offset[0]);
        offset[0] += 8;
      }

      if (bytePool.size() < 100) {
        bytePool.offer(bytes);
      }

      return retCount;
    }
    catch (Exception e) {
      throw new RuntimeException("retCount=" + retCount, e);
      //return 0;
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
