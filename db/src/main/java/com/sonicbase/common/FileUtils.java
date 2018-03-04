package com.sonicbase.common;

import java.io.File;

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
}
