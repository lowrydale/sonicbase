package com.sonicbase.common;

/**
 * Created by lowryda on 4/28/17.
 */
public class WindowsTerminal {

  /*
  Command to geerate header:
    javah -classpath db/target/sonicbase-0.9.1.jar com.sonicbase.common.WindowsTerminal
   */

  static {
    System.loadLibrary("win-util"); // Load native library at runtime
    // hello.dll (Windows) or libhello.so (Unixes)
  }

  public native void enableAnsi();

  public native String getConsoleSize();
}
