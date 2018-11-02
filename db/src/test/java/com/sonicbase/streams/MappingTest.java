/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.streams;

import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;

public class MappingTest {

  @Test
  public void test() {
    Map<String, Object> yamlConfig = new Yaml().loadAs(MappingTest.class.getResourceAsStream("/config/es-mapping.yaml"), Map.class);
    List<Map<String, Object>> tables = (List<Map<String, Object>>) yamlConfig.get("tables");
    for (Map<String, Object> table : tables) {
      String tableStr = (String) table.get("table");
      System.out.println("test");
    }
  }
}
