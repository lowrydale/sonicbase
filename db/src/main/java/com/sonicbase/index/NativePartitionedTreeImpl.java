package com.sonicbase.index;

import com.sonicbase.common.FileUtils;
import com.sonicbase.query.DatabaseException;
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
    try {
      synchronized (mutex) {
        if (!initializedNative) {
          String libName = "";
          String name = ManagementFactory.getRuntimeMXBean().getName();
          String pid = name.split("@")[0];

          if (isWindows()) {
            libName = "/win/SonicBase.dll";
          }
          else if (isCygwin()) {
            libName = "/win/SonicBase.dll";
          }
          else if (isMac()) {
            libName = "/mac/SonicBase.so";
          }
          else if (isUnix()) {
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
    catch (IOException e) {
      logger.error("Unable to load native library", e);
      throw new DatabaseException(e);
    }

    this.port = port;
    this.index = index;
    this.indexId = initIndex();
  }

  @Override
  public Object put(Object[] key, Object value) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = put(indexId, keyBytes, (long)value);
    if (ret == -1) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  public byte[] putBytes(Object[] key, byte[] value) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = putBytes(indexId, keyBytes, value);
    if (ret == null) {
      index.getSizeObj().incrementAndGet();
      return null;
    }
    return ret;
  }

  @Override
  public Object get(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = get(indexId, keyBytes);
    if (-1 == ret) {
      return null;
    }
    return ret;
  }

  @Override
  public Object remove(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    long ret = remove(indexId, keyBytes);
    if (ret != -1) {
      index.getSizeObj().decrementAndGet();
      return ret;
    }
    return null;
  }

  @Override
  public int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);
      long[] ret = tailBlockArray(indexId, keyBytes, count, first);
      int retCount = ret.length / 2;
      for (int i = 0; i < retCount; i++) {
        //keys[i][0] = (Long)ret[i];// = new Object[]{ret[i]};
        keys[i] = new Object[]{ret[i]};// = new Object[]{ret[i]};
        values[i] = ret[i + retCount];
      }
      return retCount;
      //return parseResults(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int tailBlockBytes(Object[] startKey, int count, boolean first, Object[][] keys, byte[][] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);

      byte[] ret = tailBlockBytes(indexId, keyBytes, count, first);
      return parseResultsBytes(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values) {
    try {
      byte[] keyBytes = new byte[8];
      writeLong(keyBytes, (long)startKey[0]);
      long[] ret = headBlockArray(indexId, keyBytes, count, first);
      int retCount = ret.length / 2;
      for (int i = 0; i < retCount; i++) {
      //  keys[i][0] = (Long)ret[i];
        keys[i] = new Object[]{ret[i]};
        values[i] = ret[i + retCount];
      }
      return retCount;
      //return parseResults(ret, keys, values);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map.Entry<Object[], Object> higherEntry(Object[] key) {
    long[] ret = higherEntry(indexId, (long)key[0]);
    if (ret == null) {
      return null;
    }
    return new Index.MyEntry(new Object[]{ret[0]}, ret[1]);
  }

  @Override
  public Map.Entry<Object[], Object> lowerEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = lowerEntry(indexId, keyBytes);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> floorEntry(Object[] key) {
    byte[] keyBytes = new byte[8];
    writeLong(keyBytes, (long)key[0]);

    byte[] ret = floorEntry(indexId, keyBytes);
    return parseResults(ret);
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
        if ((long) entry.getKey()[0] != (long) key[0]) {
          return ret;
        }
        ret.add(entry);
      }
      return ret;
    }
  }


  @Override
  public Map.Entry<Object[], Object> ceilingEntry(Object[] key) {
    long[] ret = ceilingEntry(indexId, (long)key[0]);
    if (ret == null) {
      return null;
    }
    return new Index.MyEntry(new Object[]{ret[0]}, ret[1]);
  }

  @Override
  public Map.Entry<Object[], Object> lastEntry() {
    byte[] ret = lastEntry2(indexId);
    return parseResults(ret);
  }

  @Override
  public Map.Entry<Object[], Object> firstEntry() {
    byte[] ret = firstEntry2(indexId);
    return parseResults(ret);
  }

  @Override
  public void clear() {
    clear(indexId);
  }

  private Map.Entry<Object[],Object> parseResults(byte[] ret) {
    try {
      if (ret == null) {
        return null;
      }
      int offset = 0;
      int len = readInt(ret, offset);
      offset += 4;
      if (len == 0) {
        return null;
      }
      long key = readLong(ret, offset);
      offset += 8;
      long value = readLong(ret, offset);
      offset += 8;
      return new Index.MyEntry(new Object[]{key}, value);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private int parseResults(byte[] results, Object[][] keys, long[] values) throws IOException {
    int offset = 0;
    int count = readInt(results, offset);
    offset += 4;
    long[] ret = new long[count];
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      offset += 4;
      long key = readLong(results, offset);
      offset += 8;
      long value = readLong(results, offset);
      offset += 8;
      keys[i] = new Object[]{key};
      values[i] = value;
      ret[i] = key;
    }
    return count;
  }

  private int parseResultsBytes(byte[] results, Object[][] keys, byte[][] values) throws IOException {
    int offset = 0;
    int count = readInt(results, offset);
    long[] ret = new long[count];
    for (int i = 0; i < count; i++) {
      int len = readInt(results, offset);
      offset += 4;
      long key = readLong(results, offset);
      offset += 8;
      len = readInt(results, offset);
      byte[] bytes = new byte[len];
      System.arraycopy(results, offset, bytes, 0, len);
      offset += len;
      keys[i] = new Object[]{key};
      values[i] = bytes;
      ret[i] = key;
    }
    return count;
  }

//  private List<Map.Entry<Object[], Object>> parseResultsList(byte[] results) throws IOException {
//    AtomicInteger offset = new AtomicInteger();
//    int count = readInt(results, offset);
//    List<Map.Entry<Object[], Object>> retList = new ArrayList<>();
//    for (int i = 0; i < count; i++) {
//      int len = readInt(results, offset);
//      long key = readLong(results, offset);
//      long value = readLong(results, offset);
//      Index.MyEntry<Object[], Object> entry = new Index.MyEntry<>(new Object[]{key}, value);
//      retList.add(entry);
//    }
//    return retList;
//  }

  public final long readLong(byte[] bytes, int offset) throws IOException {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (bytes[i + offset] & 0xFF);
    }
    return result;
  }

  public final void writeLong(byte[] bytes, long v) {
    bytes[0] = (byte)(v >>> 56);
    bytes[1] = (byte)(v >>> 48);
    bytes[2] = (byte)(v >>> 40);
    bytes[3] = (byte)(v >>> 32);
    bytes[4] = (byte)(v >>> 24);
    bytes[5] = (byte)(v >>> 16);
    bytes[6] = (byte)(v >>>  8);
    bytes[7] = (byte)(v >>>  0);
  }

  public final int readInt(byte[] bytes, int offset) throws IOException {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static int bytesToInt(byte[] bytes, int offset) {
    return bytes[offset] << 24 |
        (bytes[1 + offset] & 0xFF) << 16 |
        (bytes[2 + offset] & 0xFF) << 8 |
        (bytes[3 + offset] & 0xFF);
  }

  public static long bytesToLong(byte[] b, int offset) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result <<= 8;
      result |= (b[i + offset] & 0xFF);
    }
    return result;
  }

}
