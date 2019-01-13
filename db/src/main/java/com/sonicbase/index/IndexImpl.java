/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.index;

import java.util.List;
import java.util.Map;

public interface IndexImpl {

  Object put(Object[] key, Object value);

  Object get(Object[] startKey);

  Object remove(Object[] key);

  int tailBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values);

  int headBlock(Object[] startKey, int count, boolean first, Object[][] keys, long[] values);

  Map.Entry<Object[], Object> higherEntry(Object[] key);

  Map.Entry<Object[], Object> lowerEntry(Object[] key);

  Map.Entry<Object[], Object> floorEntry(Object[] key);

  List<Map.Entry<Object[], Object>> equalsEntries(Object[] key);

  Map.Entry<Object[], Object> ceilingEntry(Object[] key);

  Map.Entry<Object[], Object> lastEntry();

  Map.Entry<Object[], Object> firstEntry();

  void clear();
}
