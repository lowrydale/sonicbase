package com.sonicbase.common;

/**
 * Created by lowryda on 4/28/17.
 */
public class WindowsTerminal {

  static {
    System.loadLibrary("win-util"); // Load native library at runtime
    // hello.dll (Windows) or libhello.so (Unixes)
  }

  public native void enableAnsi();
}
