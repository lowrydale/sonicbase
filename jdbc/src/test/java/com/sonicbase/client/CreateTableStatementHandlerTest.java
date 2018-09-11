package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class CreateTableStatementHandlerTest {

  @Test
  public void test() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);

    CreateTable create = new CreateTable();
    create.setTable(new Table("table1"));
    List<ColumnDefinition> columns = new ArrayList<>();
    ColumnDefinition def = new ColumnDefinition();
    def.setColumnName("field1");
    ColDataType type = new ColDataType();
    type.setDataType("BIGINT");
    def.setColDataType(type);
    List<String> specs = new ArrayList<>();
    specs.add("auto_increment");
    specs.add("array");
    def.setColumnSpecStrings(specs);
    columns.add(def);
    List<Index> indices = new ArrayList<>();
    Index index = new Index();
    index.setColumnsNames(Collections.singletonList("field1"));
    index.setType("primary key");
    indices.add(index);
    create.setIndexes(indices);
    create.setColumnDefinitions(columns);

    final AtomicBoolean called = new AtomicBoolean();
    when(client.sendToMaster(eq("SchemaManager:createTable"), anyObject())).thenAnswer(
        invocationOnMock -> {
          called.set(true);
          ComObject ret = new ComObject();
          ret.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema((short) 1000));
          return ret.serialize();
        });
    CreateTableStatementHandler handler = new CreateTableStatementHandler(client);
    handler.execute("test", null, "create table table1", create, null, 100L, 100L, (short)100, false, null, 0);
    assertTrue(called.get());
  }
}
