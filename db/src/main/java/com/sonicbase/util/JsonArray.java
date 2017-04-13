package com.sonicbase.util;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:16 PM
 */

import com.google.gson.JsonElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JsonArray {
  private List<Object> list = new ArrayList<Object>();

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");

  public int size() {
    return list.size();
  }

  public boolean isDict(int offset) {
    return list.get(offset) instanceof Map;
  }

  public boolean isArray(int offset) {
    return list.get(offset) instanceof JsonArray;
  }

  public int getInt(int offset) {
    return ((Number) list.get(offset)).intValue();
  }

  public long getLong(int offset) {
    return ((Number) list.get(offset)).longValue();
  }

  public String getString(int offset) {
    return (String) list.get(offset);
  }

  public JsonDict getDict(int offset) {
    return (JsonDict) list.get(offset);
  }

  public JsonArray getArray(int offset) {
    return (JsonArray) list.get(offset);
  }

  public JsonDict addDict() {
    JsonDict ret = new JsonDict();
    list.add(ret);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
    return ret;
  }

  public JsonArray addArray() {
    JsonArray ret = new JsonArray();
    list.add(ret);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
    return ret;
  }

  public String toString() {
    StringBuilder ret = new StringBuilder();
    toString(ret, true);
    return ret.toString();
  }

  public void toString(StringBuilder builder, boolean prettyPrint) {
    boolean first = true;
    builder.append("[");
    if (prettyPrint) {
      builder.append("\n");
    }
    for (Object entry : list) {
      if (!first) {
        builder.append(",");
        if (prettyPrint) {
          builder.append("\n");
        }
      }
      first = false;
      if (entry instanceof JsonArray) {
        ((JsonArray) entry).toString(builder, prettyPrint);
      }
      else if (entry instanceof JsonDict) {
        ((JsonDict) entry).toString(builder, prettyPrint);
      }
      else if (entry instanceof Long) {
        builder.append((Long) entry);
      }
      else if (entry instanceof Double) {
        builder.append((Double) entry);
      }
      else if (entry instanceof Boolean) {
        builder.append(((Boolean)entry).booleanValue() ? "true" : "false");
      }
      else if (entry instanceof String) {
        builder.append("\"").append((String) entry).append("\"");
      }
    }

    builder.append("]");
    if (prettyPrint) {
      builder.append("\n");
    }
  }

  public JsonArray fromJson(com.google.gson.JsonArray array) {
    list = new ArrayList<Object>();
    for (int i = 0; i < array.size(); i++) {
      JsonElement elem = array.get(i);
      if (elem.isJsonArray()) {
        list.add(new JsonArray().fromJson(elem.getAsJsonArray()));
      }
      else if (elem.isJsonObject()) {
        list.add(new JsonDict().fromJson(elem.getAsJsonObject()));
      }
      else if(elem.isJsonPrimitive()) {
        if (elem.getAsJsonPrimitive().isNumber()) {
          list.add(elem.getAsJsonPrimitive().getAsNumber());
        }
        else if (elem.getAsJsonPrimitive().isString()) {
          list.add(elem.getAsJsonPrimitive().getAsString());
        }
        else if (elem.getAsJsonPrimitive().isBoolean()) {
          list.add(elem.getAsJsonPrimitive().getAsBoolean());
        }
      }
    }
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
    return this;
  }

  public void add(Integer integer) {
    Number number = new Integer(integer);
    list.add(number);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
  }

  public void add(String str) {
    list.add(str);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
  }

  public void add(long org) {
    Number number = new Long(org);
    list.add(number);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
  }

  public void add(boolean org) {
    boolean bool = new Boolean(org);
    list.add(bool);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
  }

  public JsonDict addDict(JsonDict dict) {
    list.add(dict);
    if (list.size() > 100000) {
      logger.error("JsonArray size > 100k: ", new Throwable());
    }
    return dict;
  }

  public void remove(int i) {
    list.remove(i);
  }

  public int[] toIntArray() {
    int[] ret = new int[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object obj = list.get(i);
      if (obj instanceof Number) {
        ret[i] = ((Number)obj).intValue();
      }
      else if (obj instanceof Long) {
        ret[i] = (int)(long)(Long)obj;
      }
    }
    return ret;
  }

  public JsonDict[] toDictArray() {
    JsonDict[] ret = new JsonDict[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object obj = list.get(i);
      if (obj instanceof JsonDict) {
        ret[i] = (JsonDict)obj;
      }
    }
    return ret;
  }

  public long[] toLongArray() {
    long[] ret = new long[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object obj = list.get(i);
      if (obj instanceof Number) {
        ret[i] = ((Number)obj).longValue();
      }
      else if (obj instanceof Long) {
        ret[i] = (long)(Long)obj;
      }
    }
    return ret;
  }

  public boolean[] toBooleanArray() {
    boolean[] ret = new boolean[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object obj = list.get(i);
      if (obj instanceof Boolean) {
        ret[i] = ((Boolean)obj).booleanValue();
      }
    }
    return ret;
  }

  public String[] toStringArray() {
    String[] ret = new String[list.size()];
    for (int i = 0; i < list.size(); i++) {
      Object obj = list.get(i);
      if (obj instanceof String) {
        ret[i] = ((String)obj);
      }
    }
    return ret;
  }

  public void add(boolean[] values) {
    for (boolean value : values) {
      add(value);
    }
  }

  public void add(long[] values) {
    for (long value : values) {
      add(value);
    }
  }

  public void add(String[] values) {
    for (String value : values) {
      add(value);
    }
  }

  public void add(int[] values) {
    for (int value : values) {
      add(value);
    }
  }

  public void add(JsonDict[] array) {
    for (JsonDict value : array) {
      addDict(value);
    }
  }

}

