package com.sonicbase.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {

  public static String getStackTrace(Throwable t) {
    //t.fillInStackTrace();
    StringWriter sWriter = new StringWriter();
    PrintWriter writer = new PrintWriter(sWriter);
    t.printStackTrace(writer);
    writer.close();
    return sWriter.toString();
  }
}
