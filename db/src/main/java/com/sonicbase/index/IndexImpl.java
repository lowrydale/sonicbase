package com.sonicbase.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface IndexImpl {
  void clear();

  Object get(Object[] key);

  Object put(Object[] key, Object id);

  Object remove(Object[] key);

  Map.Entry<Object[], Object> ceilingEntry(Object[] key);

  List<Map.Entry<Object[], Object>> equalsEntries(Object[] key);

  Map.Entry<Object[], Object> floorEntry(Object[] key);

  Map.Entry<Object[], Object> lowerEntry(Object[] key);

  Map.Entry<Object[], Object> higherEntry(Object[] key);

  Iterable<Object> values();

  boolean visitTailMap(Object[] key, Index.Visitor visitor) throws IOException;

  boolean visitHeadMap(Object[] key, Index.Visitor visitor) throws IOException;

  Map.Entry<Object[], Object> lastEntry();

  Map.Entry<Object[], Object> firstEntry();
}
