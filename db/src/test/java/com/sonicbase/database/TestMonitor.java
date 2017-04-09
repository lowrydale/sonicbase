package com.sonicbase.database;

import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lowryda on 4/8/17.
 */
public class TestMonitor {

  @Test
  public void test() {
    String ch = "   [Eden: 58.0M(58.0M)->0.0B(58.0M) Survivors: 1024.0K->1024.0K Heap: 81.9M(179.0M)->23.9M(179.0M)]";

    int pos = ch.indexOf("Heap:");
    if (pos != -1) {
      int pos2 = ch.indexOf("(", pos);
      if (pos2 != -1) {
        String value = ch.substring(pos + "Heap:".length(), pos2).trim().toLowerCase();
        double maxGig = 0;
        if (value.contains("g")) {
          maxGig = Double.valueOf(value.substring(0, value.length() - 1));
        }
        else if (value.contains("m")) {
          maxGig = Double.valueOf(value.substring(0, value.length() - 1)) / 1000d;
        }
        else if (value.contains("t")) {
          maxGig = Double.valueOf(value.substring(0, value.length() - 1)) * 1000d;
        }
        System.out.println(maxGig);
      }
    }
  }

  @Test
  public void testNet() {
    String line = " 0.60      0.53";
    List<Double> transRate = new ArrayList<>();
    List<Double> recRate = new ArrayList<>();
    double trans = 0;
    double rec = 0;
    String[] parts = line.trim().split("\\s+");
    for (int i = 0; i < parts.length; i++) {

      if (i % 2 == 0) {
        rec += Double.valueOf(parts[i]);
      }
      else if (i % 2 == 1) {
        trans += Double.valueOf(parts[i]);
      }
    }
    transRate.add(trans);
    recRate.add(rec);
    if (transRate.size() > 10) {
      transRate.remove(0);
    }
    if (recRate.size() > 10) {
      recRate.remove(0);
    }
    Double total = 0d;
    for (Double currRec : recRate) {
      total += currRec;
    }
    System.out.println(total / recRate.size());

    total = 0d;
    for (Double currTrans : transRate) {
      total += currTrans;
    }
    System.out.println(total / transRate.size());
  }

  @Test
  public void testFormat() {
    System.out.println(String.format("%s\t%.2f%%\t%.2fg\t%.2fg\t%.2fg\t%.2fKB\t%.2fKB\t%s",
        "12.0.0.1", 200.3, 0.003, 0.24, 0.59, 100.2, 100.2, "60g"));

  }
}
