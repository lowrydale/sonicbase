package com.sonicbase.util;

/**
 * User: lowryda
 * Date: 9/30/14
 * Time: 5:15 PM
 */

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.*;

public class JsonDict {
  private Map<String, Object> dict = new HashMap<String, Object>();

  public JsonDict() {

  }

  public JsonDict(String jsonData) {
    fromString(jsonData);
  }

  public boolean isDict(String key) {
    return dict.get(key) instanceof Map;
  }

  public boolean isArray(String key) {
    return dict.get(key) instanceof JsonArray;
  }

  public boolean isString(String key) {
    return dict.get(key) instanceof String;
  }

  public boolean isLong(String key) {
    return dict.get(key) instanceof Long;
  }

  public boolean isDouble(String key) {
    return dict.get(key) instanceof Double;
  }

  public boolean isBoolean(String key) {
    return dict.get(key) instanceof Boolean;
  }

  public JsonDict getDict(String key) {
    return (JsonDict) dict.get(key);
  }

  public JsonDict putDict(String key) {
    JsonDict ret = new JsonDict();
    dict.put(key, ret);
    return ret;
  }

  public JsonDict putDict(String key, JsonDict rhs) {
    dict.put(key, rhs);
    return rhs;
  }

  public JsonArray putArray(String key) {
    JsonArray ret = new JsonArray();
    dict.put(key, ret);
    return ret;
  }

  public JsonArray putArray(String key, JsonArray array) {
    dict.put(key, array);
    return array;
  }

  public JsonArray getArray(String key) {
    return (JsonArray)dict.get(key);
  }

  public JsonDict put(String key, String value) {
    dict.put(key, value);
    return this;
  }

  public JsonDict put(String key, long value) {
    dict.put(key, value);
    return this;
  }

  public JsonDict put(String key, double value) {
    dict.put(key, value);
    return this;
  }

  public JsonDict put(String key, boolean value) {
    dict.put(key, value);
    return this;
  }

  public String getString(String key) {
    Object ret = dict.get(key);
    if (ret == null) {
      return null;
    }
    if (ret instanceof Long) {
      return String.valueOf((Long)ret);
    }
    if (ret instanceof Double) {
      return String.valueOf((Double)ret);
    }
    if (ret instanceof String) {
      return (String)dict.get(key);
    }
    throw new DatabaseException("Unsupported datatype: " + ret.getClass().getName());
  }

  public Long getLong(String key) {
    Object ret = dict.get(key);
    if (ret == null) {
      return null;
    }
    if (ret instanceof String) {
      return Long.valueOf((String)ret);
    }
    if (ret instanceof Long) {
      return (Long)ret;
    }
    if (ret instanceof Double) {
      return (long)(double)(Double)ret;
    }
    throw new DatabaseException("Unsupported datatype: " + ret.getClass().getName());
  }

  public int getInt(String key) {
    return (int)(long)getLong(key);
  }

  public Double getDouble(String key) {
    Object ret = dict.get(key);
    if (ret == null) {
      return null;
    }
    if (ret instanceof String) {
      return Double.valueOf((String)ret);
    }
    if (ret instanceof Long) {
      return (double)(long)(Long)ret;
    }
    if (ret instanceof Double) {
      return (Double)ret;
    }
    throw new DatabaseException("Unsupported datatype: " + ret.getClass().getName());
  }

  public Boolean getBoolean(String key) {
    return (Boolean)dict.get(key);
  }

  public String toString() {
    return toString(true);
  }

  public String toString(boolean prettyPrint) {
    StringBuilder builder = new StringBuilder();
    toString(builder, prettyPrint);
    return builder.toString();
  }

  public void toString(StringBuilder builder, boolean prettyPrint) {
    builder.append("{");
    if (prettyPrint) {
      builder.append("\n");
    }
    boolean first = true;
    for (Map.Entry<String, Object> entry : dict.entrySet()) {
      if (!first) {
        builder.append(",");
        if (prettyPrint) {
          builder.append("\n");
        }
      }
      first = false;
      builder.append("\"").append(entry.getKey()).append("\":");
      if (prettyPrint) {
        builder.append(" ");
      }
      if (entry.getValue() == null) {
        builder.append("null");
      }
      else if (entry.getValue() instanceof JsonDict) {
        ((JsonDict)entry.getValue()).toString(builder, prettyPrint);
      }
      else if (entry.getValue() instanceof JsonArray) {
        ((JsonArray)entry.getValue()).toString(builder, prettyPrint);
      }
      else if (entry.getValue() instanceof String) {
        String value = (String) entry.getValue();
        value = StringEscapeUtils.escapeJson(value);
        builder.append("\"").append(value).append("\"");
      }
      else if (entry.getValue() instanceof Long) {
        builder.append((Long) entry.getValue());
      }
      else if (entry.getValue() instanceof Double) {
        builder.append((Double) entry.getValue());
      }
      else if (entry.getValue() instanceof Boolean) {
        builder.append((Boolean) entry.getValue());
      }
    }
    builder.append("}");
    if (prettyPrint) {
      builder.append("\n");
    }
  }

  public JsonDict fromString(String jsonStr) {
    JsonParser parser = new JsonParser();
    JsonElement elem = parser.parse(jsonStr);
    JsonObject root = elem.getAsJsonObject();
    return fromJson(root);
  }

  public JsonDict fromJson(JsonObject obj) {
    dict = new HashMap<String, Object>();
    for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
      String key = entry.getKey();
      JsonElement innerElem = entry.getValue();
      if (innerElem.isJsonArray()) {
        dict.put(key, new JsonArray().fromJson(innerElem.getAsJsonArray()));
      }
      else if (innerElem.isJsonObject()) {
        dict.put(key, new JsonDict().fromJson(innerElem.getAsJsonObject()));
      }
      else if (innerElem.isJsonPrimitive()) {
        JsonPrimitive prim = innerElem.getAsJsonPrimitive();
        if (prim.isString()) {
          dict.put(key, prim.getAsString());
        }
        else if (prim.isBoolean()) {
          dict.put(key, prim.getAsBoolean());
        }
        else if (prim.isNumber()) {
          try {
            dict.put(key, prim.getAsNumber().longValue());
          }
          catch (NumberFormatException e1) {
            try {
              dict.put(key, prim.getAsNumber().doubleValue());
            }
            catch (NumberFormatException e2) {
              throw e2;
            }
          }
        }
      }
    }
    return this;
  }

  public Set<Map.Entry<String, Object>> getEntrySet() {
    return dict.entrySet();
  }

  public boolean hasKey(String key) {
    if (dict == null) {
      return false;
    }
    return dict.containsKey(key);
  }

  public int size() {
    return dict.size();
  }

  public List<String> keys() {
    List<String> ret = new ArrayList<String>();
    Set<String> keys = dict.keySet();
    for (String key : keys) {
      ret.add(key);
    }
    return ret;
  }

  public void add(JsonDict rhs) {
    for (Map.Entry<String, Object> entry : rhs.getEntrySet()) {
      if (entry.getValue() instanceof JsonDict) {
        putDict(entry.getKey(), (JsonDict)entry.getValue());
      }
      else if (entry.getValue() instanceof JsonArray) {
        putArray(entry.getKey(), (JsonArray)entry.getValue());
      }
      else if (entry.getValue() instanceof String) {
        put(entry.getKey(), (String)entry.getValue());
      }
      else if (entry.getValue() instanceof Long) {
        put(entry.getKey(), (Long)entry.getValue());
      }
      else if (entry.getValue() instanceof Double) {
        put(entry.getKey(), (Double)entry.getValue());
      }
      else if (entry.getValue() instanceof Boolean) {
        put(entry.getKey(), (Boolean)entry.getValue());
      }
    }
  }

  public JsonArray putArray(String key, List<JsonDict> list) {
    JsonArray array = new JsonArray();
    for (JsonDict dict : list) {
      array.addDict(dict);
    }
    dict.put(key, array);
    return array;
  }

  public void remove(String key) {
    dict.remove(key);
  }

}
