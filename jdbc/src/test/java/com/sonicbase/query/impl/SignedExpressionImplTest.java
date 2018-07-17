/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.query.impl;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.Record;
import com.sonicbase.schema.DataType;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.TestUtils;
import org.testng.annotations.Test;

import java.io.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SignedExpressionImplTest {

  @Test
  public void test() throws UnsupportedEncodingException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = TestUtils.createTable();
    IndexSchema indexSchema = TestUtils.createIndexSchema(tableSchema);
    TestUtils.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    byte[][] records = TestUtils.createRecords(common, tableSchema, 10);

    SignedExpressionImpl expression = new SignedExpressionImpl();
    ConstantImpl constant = new ConstantImpl();
    constant.setSqlType(DataType.Type.BIGINT.getValue());
    constant.setValue(200L);
    expression.setExpression(constant);
    expression.setNegative(true);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytesOut);
    expression.serialize((short)100, out);

    expression = new SignedExpressionImpl();
    expression.deserialize((short)100, new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertTrue(expression.isNegative());
    Record record = new Record("test", common, records[0]);
    Object ret = expression.evaluateSingleRecord(new TableSchema[]{tableSchema}, new Record[]{record}, null);

    assertEquals((long)ret, -200L);
  }
}
