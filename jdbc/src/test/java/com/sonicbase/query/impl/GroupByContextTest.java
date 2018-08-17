package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

public class GroupByContextTest {

  @Test
  public void test() throws IOException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    TestUtils.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    List<GroupByContext.FieldContext> fieldContexts = new ArrayList<>();
    GroupByContext.FieldContext context = new GroupByContext.FieldContext();
    context.setFieldName("field1");
    context.setComparator(DataType.getLongComparator());
    context.setDataType(DataType.Type.BIGINT);
    context.setFieldOffset(1);
    fieldContexts.add(context);
    GroupByContext groupByContext = new GroupByContext(fieldContexts);

    Counter template = new Counter();
    template.setColumnName("field1");
    template.setTableName("table1");
    template.setDataType(DataType.Type.BIGINT);
    groupByContext.addCounterTemplate(template);

    groupByContext.addGroupContext(new Object[]{200L});
    byte[] bytes = groupByContext.serialize(common);

    groupByContext = new GroupByContext();
    groupByContext.deserialize(bytes, common);

    Map<String, Map<Object[], GroupByContext.GroupCounter>> counters = groupByContext.getGroupCounters();

    assertEquals(groupByContext.getFieldContexts().size(), 1);

    assertNotNull(counters);
  }
}
