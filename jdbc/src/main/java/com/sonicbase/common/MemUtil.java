package com.sonicbase.common;

/**
 * Created by lowryda on 7/28/17.
 */
public class MemUtil {

  public static double getMemValue(String memStr) {
    int qualifierPos = memStr.toLowerCase().indexOf("m");
    if (qualifierPos == -1) {
      qualifierPos = memStr.toLowerCase().indexOf("g");
      if (qualifierPos == -1) {
        qualifierPos = memStr.toLowerCase().indexOf("t");
        if (qualifierPos == -1) {
          qualifierPos = memStr.toLowerCase().indexOf("k");
          if (qualifierPos == -1) {
            qualifierPos = memStr.toLowerCase().indexOf("b");
          }
        }
      }
    }
    double value = 0;
    if (qualifierPos == -1) {
      value = Double.valueOf(memStr.trim());
      value = value / 1024d / 1024d / 1024d;
    }
    else {
      char qualifier = memStr.toLowerCase().charAt(qualifierPos);
      value = Double.valueOf(memStr.substring(0, qualifierPos).trim());
      if (qualifier == 't') {
        value = value * 1024d;
      }
      else if (qualifier == 'm') {
        value = value / 1024d;
      }
      else if (qualifier == 'k') {
        value = value / 1024d / 1024d;
      }
    }
    return value;
  }
}
