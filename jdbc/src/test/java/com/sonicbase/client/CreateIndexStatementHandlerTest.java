package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.Index;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class CreateIndexStatementHandlerTest {

  @Test
  public void test() throws SQLException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    CreateIndex create = new CreateIndex();
    Index index = new Index();
    index.setName("index1");
    List<String> columns = new ArrayList<>();
    columns.add("field1");
    columns.add("field2");
    index.setColumnsNames(columns);
    create.setIndex(index);
    create.setTable(new Table("table1"));
    CreateIndexStatementHandler handler = new CreateIndexStatementHandler(client);

    final AtomicBoolean called = new AtomicBoolean();
    when(client.sendToMaster(eq("SchemaManager:createIndex"), anyObject())).thenAnswer(
        (Answer) invocationOnMock -> {
          called.set(true);
          ComObject ret = new ComObject(2);
          ret.put(ComObject.Tag.SCHEMA_BYTES, common.serializeSchema((short) 1000));
          return ret;
        });

    handler.execute("test", null, "create index index1 on table1 field1, field2", create, null, 100L, 100L, (short)100, false, null, 0);
    assertTrue(called.get());
  }
}
