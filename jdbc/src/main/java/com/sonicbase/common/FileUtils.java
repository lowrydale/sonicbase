package com.sonicbase.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class FileUtils {

  public static long sizeOfDirectory(File dir) {
    File[] files = dir.listFiles();
    if (files == null) {
      return 0;
    }
    long ret = 0;
    for (File file : files) {
      try {
        if (file.isDirectory()) {
          ret += sizeOfDirectory(file);
        }
        else {
          ret += file.length();
        }
      }
      catch (Exception e) {
      }
    }
    return ret;
  }

  public static void deleteDirectory(File file) throws IOException {

    File[] files = file.listFiles();
    if (files == null) {
      return;
    }
    for (File childFile : files) {
      if (childFile.isDirectory()) {
        deleteDirectory(childFile);
      }
      else {
        if (childFile.exists()) {
          Files.delete(childFile.toPath());
        }
      }
    }

    if (file.exists()) {
      Files.delete(file.toPath());
    }
  }
}
