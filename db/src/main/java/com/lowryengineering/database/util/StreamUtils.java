package com.lowryengineering.database.util;

import java.io.*;

/**
 * User: lowryda
 * Date: 4/12/13
 * Time: 2:47 PM
 */
public class StreamUtils {


  public static String inputStreamToString(InputStream inStream) throws IOException {
    StringBuilder builder = new StringBuilder();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
    boolean first = true;
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      if (!first) {
        builder.append("\n");
      }
      first = false;
      builder.append(line);
    }
    reader.close();
    return builder.toString();
  }

  public static String readerToString(Reader inStream) throws IOException {
    StringBuilder builder = new StringBuilder();
    BufferedReader reader = new BufferedReader(inStream);
    boolean first = true;
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      if (!first) {
        builder.append("\n");
      }
      first = false;
      builder.append(line);
    }
    reader.close();
    return builder.toString();
  }

  private static final int DEFAULT_BUFFER_SIZE = 8192;

  public static void copyStream(InputStream inStream, OutputStream outStream) throws IOException {
    int bufferSize;
    try {
      bufferSize = Math.max(DEFAULT_BUFFER_SIZE, inStream.available());
    }
    catch (IOException e) {
      // this happens with new FileInputStream(new File("/proc/meminfo")).available() for instance
      bufferSize = DEFAULT_BUFFER_SIZE;
    }
    copyStream(inStream, outStream, bufferSize);
  }

  public static void copyStream(InputStream inStream, OutputStream outStream, int bufferSize) throws IOException {
    final byte[] buffer = new byte[bufferSize];
    while (true) {
      final int read = inStream.read(buffer);
      if (read == -1) {
        break;
      }
      outStream.write(buffer, 0, read);
    }
    inStream.close();
    outStream.close();
  }

  public static byte[] inputStreamToBytes(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    copyStream(in, out);
    return out.toByteArray();
  }

}
