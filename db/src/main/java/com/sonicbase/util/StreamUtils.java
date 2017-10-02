package com.sonicbase.util;

import org.apache.commons.io.IOUtils;

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
    IOUtils.copy(inStream, outStream);
  }

  public static byte[] inputStreamToBytes(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    copyStream(in, out);
    return out.toByteArray();
  }

  public static void copyFile(File srcFile, File destFile) throws IOException {
    destFile.delete();
    destFile.getParentFile().mkdirs();

    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(srcFile));
      BufferedOutputStream out = new BufferedOutputStream((new FileOutputStream(destFile)))) {
      IOUtils.copy(in, out);
    }
  }

}
