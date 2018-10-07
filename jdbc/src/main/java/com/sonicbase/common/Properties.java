/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.query.DatabaseException;
import org.yaml.snakeyaml.Yaml;

import java.io.StringReader;
import java.util.Map;

public class Properties {
  private Map<String, Object> yamlConfig;

  public Properties() {
  }

  public Properties(Map<String, Object> map) {
    this.yamlConfig = map;
  }

  public Properties(String rawConfig) {
    try {
      yamlConfig = new Yaml().loadAs(new StringReader(rawConfig), Map.class);
    }
    catch (Exception e) {
      try {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonConfig = (ObjectNode) mapper.readTree(rawConfig);
        throw new DatabaseException("json format no longer supported. Use yaml instead");
      }
      catch (Exception e1) {
        throw new DatabaseException(e);
      }
    }
  }

  public void putMap(Map<String, Object> map) {
    this.yamlConfig = map;
  }

  public Map<String, Object> getMap() {
    return yamlConfig;
  }

  protected void setMap(Map<String, Object> map) {
    this.yamlConfig = map;
  }

  public void put(String tag, int value) {
    yamlConfig.put(tag, value);
  }

  public void put(String tag, String value) {
    yamlConfig.put(tag, value);
  }

  public void put(String tag, boolean value) {
    yamlConfig.put(tag, value);
  }

  public Long getLong(String tag) {
    return (Long) yamlConfig.get(tag);
  }

  public Integer getInt(String tag) {
    return (Integer) yamlConfig.get(tag);
  }

  public Boolean getBoolean(String tag) {
    return (Boolean) yamlConfig.get(tag);
  }

  public String getString(String tag) {
    return (String) yamlConfig.get(tag);
  }

}
