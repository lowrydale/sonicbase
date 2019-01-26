/* © 2019 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import org.testng.annotations.Test;

public class OSStatsManagerImplTest {

  @Test
  public void test() {
    String ch = "153.889: [GC (Allocation Failure) [PSYoungGen: 2658451K->65976K(2690560K)] 2830514K->238047K(3554304K), 0.0261834 secs] [Times: user=0.16 sys=0.01, real=0.03 secs]";
    if (ch.contains("PSYoungGen:") && ch.contains("Times:")) {
      int pos = ch.indexOf("]");
      int pos2 = ch.indexOf("->", pos + 1);
      String max = ch.substring(pos + 1, pos2);
      max = max.trim();
      String maxValue = max.substring(0, max.length() - 1);
      maxValue.trim();
      double maxGig = Double.parseDouble(maxValue);
      if (max.endsWith("K") || max.endsWith("k")) {
        maxGig = maxGig / 1000d / 1000d;
      }
      else if (max.endsWith("G") || max.endsWith("g")) {
        maxGig = maxGig;
      }
      else if (max.endsWith("M") || max.endsWith("m")) {
        maxGig = maxGig / 1000d;
      }
      else if (max.endsWith("T") || max.endsWith("t")) {
        maxGig = maxGig * 1000d;
      }
      System.out.println(maxGig);

      pos = ch.indexOf("->", pos);
      pos2 = ch.indexOf("(", pos);
      String min = ch.substring(pos + 2, pos2);
      min = min.trim();
      String minValue = min.substring(0, min.length() - 1);
      minValue.trim();
      double minGig = Double.parseDouble(minValue);
      if (min.endsWith("K") || min.endsWith("k")) {
        minGig /= 1000d / 1000d;
      }
      else if (min.endsWith("G") || min.endsWith("g")) {
        minGig = minGig;
      }
      else if (min.endsWith("M") || min.endsWith("m")) {
        minGig /= 1000d;
      }
      else if (min.endsWith("T") || min.endsWith("t")) {
        minGig *= 1000d;
      }
      System.out.println(minGig);

    }
  }
}
