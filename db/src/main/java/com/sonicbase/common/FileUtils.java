package com.sonicbase.common;

import java.io.File;
import java.io.IOException;

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


  public static void delete(File file) throws IOException {

    File[] files = file.listFiles();
    if (files == null) {
      return;
    }
    for (File childFile : files) {
      if (childFile.isDirectory()) {
        delete(childFile);
      }
      else {
        if (!childFile.delete()) {
          throw new IOException();
        }
      }
    }

    if (!file.delete()) {
      throw new IOException();
    }
  }
}
