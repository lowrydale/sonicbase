/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.client;

import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.jdbcdriver.ParameterHandler;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.BinaryExpression;
import com.sonicbase.query.impl.*;
import com.sonicbase.schema.IndexSchema;
import com.sonicbase.schema.TableSchema;
import com.sonicbase.util.ClientTestUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import org.testng.annotations.Test;

import java.io.StringReader;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class UpdateStatementHandlerTest {

  @Test
  public void test() throws JSQLParserException {
    DatabaseClient client = mock(DatabaseClient.class);
    DatabaseCommon common = new DatabaseCommon();
    when(client.getCommon()).thenReturn(common);
    TableSchema tableSchema = ClientTestUtils.createTable();
    IndexSchema indexSchema = ClientTestUtils.createIndexSchema(tableSchema);
    ClientTestUtils.createStringIndexSchema(tableSchema);
    common.getTables("test").put(tableSchema.getName(), tableSchema);
    common.getTablesById("test").put(tableSchema.getTableId(), tableSchema);

    UpdateStatementHandler handler = new UpdateStatementHandler(client);

    String sql = "update persons set id=id+1 where id<5";
    CCJSqlParserManager parser = new CCJSqlParserManager();
    Statement statement = parser.parse(new StringReader(sql));
    UpdateStatementImpl updateStatement = new UpdateStatementImpl(client){
      public Object execute(String dbName, String sqlToUse, SelectStatementImpl.Explain explain, Long sequence0,
                            Long sequence1, Short sequence2,
                            boolean restrictToThisServer, StoredProcedureContextImpl procedureContext, int schemaRetryCount) {
          return null;
        }
      };

    Object ret = handler.execute("test", new ParameterHandler(), sql, statement,
        null, null, null, null, false,
        null, 0, updateStatement);

    System.out.println(updateStatement.toString());
    assertEquals(updateStatement.getSetExpressions().size(), 1);
    assertEquals(((ColumnImpl)((BinaryExpressionImpl)updateStatement.getSetExpressions().get(0)).getLeftExpression()).getColumnName(), "id");
    assertEquals(((ConstantImpl)((BinaryExpressionImpl)updateStatement.getSetExpressions().get(0)).getRightExpression()).getValue(), 1L);
    assertEquals(((BinaryExpressionImpl)updateStatement.getSetExpressions().get(0)).getOperator(), BinaryExpression.Operator.PLUS);
  }
}
