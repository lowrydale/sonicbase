/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.server.IndexLookupTest;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.math.analysis.interpolation.BivariateRealGridInterpolator;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ParenthesisImplTest {

  @Test
  public void test() {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = IndexLookupTest.createTable();
    IndexSchema indexSchema = IndexLookupTest.createIndexSchema(tableSchema);
    IndexLookupTest.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);


    ParenthesisImpl parens = new ParenthesisImpl();
    parens.setNot(true);
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(DataType.Type.BIGINT.getValue());
    constant.setValue(200L);
    parens.setExpression(constant);

    parens.setClient(client);
    ColumnImpl column = new ColumnImpl();
    column.setColumnName("field1");
    column.setTableName("table1");
    parens.setColumns(Collections.<ColumnImpl>singletonList(column));

    parens.setCounters(null);
    parens.setDbName("test");
    parens.setDebug(false);
    parens.setLimit(null);
    parens.setOrderByExpressions(null);
    parens.setParms(null);
    parens.setRecordCache(new ExpressionImpl.RecordCache());
    parens.setReplica(1);
    parens.setTableName("table1");
    parens.setTopLevelExpression(column);
    parens.setProbe(false);
    parens.setViewVersion(10);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    parens.serialize((short)100, out);

    parens = new ParenthesisImpl();
    parens.deserialize((short)100, new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertTrue(parens.isNot());
    assertEquals((long)((ConstantImpl)parens.getExpression()).getValue(), 200L);
  }
}
