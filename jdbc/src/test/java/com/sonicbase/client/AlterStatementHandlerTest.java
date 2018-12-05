package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class AlterStatementHandlerTest {

  @Test
  public void testAdd() throws SQLException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    AlterStatementHandler handler = new AlterStatementHandler(client);
    Alter alter = new Alter();
    alter.setColumnName("id3");
    ColDataType dataType = new ColDataType();
    dataType.setDataType("BIGINT");
    alter.setDataType(dataType);
    alter.setOperation("add");
    alter.setTable(new Table("table1"));

    final AtomicBoolean calledAdd = new AtomicBoolean();
    when(client.sendToMaster(eq("SchemaManager:addColumn"), anyObject())).thenAnswer(invocationOnMock -> {
      calledAdd.set(true);
      ComObject ret = new ComObject(1);
      ret.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema((short) 1000));
      return ret.serialize();
    });

    handler.execute("test", null, "alter table addColumn add column id3 BIGINT", alter, null, 100L, 100L, (short)100,false, null, 0);
    assertTrue(calledAdd.get());
  }

  @Test
  public void testDrop() throws SQLException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    AlterStatementHandler handler = new AlterStatementHandler(client);
    Alter alter = new Alter();
    alter.setColumnName("id3");
    ColDataType dataType = new ColDataType();
    dataType.setDataType("BIGINT");
    alter.setDataType(dataType);
    alter.setOperation("drop");
    alter.setTable(new Table("table1"));

    final AtomicBoolean calledDrop = new AtomicBoolean();
    when(client.sendToMaster(eq("SchemaManager:dropColumn"), anyObject())).thenAnswer(invocationOnMock -> {
      calledDrop.set(true);
      ComObject ret = new ComObject(1);
      ret.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema((short) 1000));
      return ret.serialize();
    });

    handler.execute("test", null, "alter table addColumn drop column id3 BIGINT", alter, null, 100L, 100L, (short)100,false, null, 0);
    assertTrue(calledDrop.get());
  }
}
