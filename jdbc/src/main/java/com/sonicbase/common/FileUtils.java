package com.sonicbase.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileUtils {

  private FileUtils() {
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
