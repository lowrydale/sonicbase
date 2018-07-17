package com.sonicbase.client;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.execute.Execute;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

import java.util.concurrent.ConcurrentHashMap;

public class StatementHandlerFactory {

  private ConcurrentHashMap<String, StatementHandler> handlers = new ConcurrentHashMap<>();
  private final DatabaseClient client;

  public StatementHandlerFactory(DatabaseClient client) {
    this.client = client;
    handlers.put(Select.class.getName(), new SelectStatementHandler(client));
    handlers.put(Insert.class.getName(), new InsertStatementHandler(client));
    handlers.put(Update.class.getName(), new UpdateStatementHandler(client));
    handlers.put(CreateTable.class.getName(), new CreateTableStatementHandler(client));
    handlers.put(CreateIndex.class.getName(), new CreateIndexStatementHandler(client));
    handlers.put(Delete.class.getName(), new DeleteStatementHandler(client));
    handlers.put(Alter.class.getName(), new AlterStatementHandler(client));
    handlers.put(Drop.class.getName(), new DropStatementHandler(client));
    handlers.put(Truncate.class.getName(), new TruncateStatementHandler(client));
    handlers.put(Execute.class.getName(), new ExecuteStatementHandler(client));
//    if (statement instanceof Select) {
//      ret = doSelect(dbName, parms, disableStats ? null : sqlToUse, (Select) statement, debug, null, restrictToThisServer, procedureContext, schemaRetryCount);
//    }
//    else if (statement instanceof Insert) {
//      ret = doInsert(dbName, parms, (Insert) statement, schemaRetryCount);
//    }
//    else if (statement instanceof Update) {
//      ret = doUpdate(dbName, parms, (Update) statement, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, schemaRetryCount);
//    }
//    else if (statement instanceof CreateTable) {
//      ret = doCreateTable(dbName, (CreateTable) statement);
//    }
//    else if (statement instanceof CreateIndex) {
//      ret = doCreateIndex(dbName, (CreateIndex) statement);
//    }
//    else if (statement instanceof Delete) {
//      ret = doDelete(dbName, parms, (Delete) statement, sequence0, sequence1, sequence2, restrictToThisServer, procedureContext, schemaRetryCount);
//    }
//    else if (statement instanceof Alter) {
//      ret = doAlter(dbName, parms, (Alter) statement);
//    }
//    else if (statement instanceof DropStatementHandler) {
//      ret = doDrop(dbName, statement, schemaRetryCount);
//    }
//    else if (statement instanceof Truncate) {
//      ret = doTruncateTable(dbName, (Truncate) statement, schemaRetryCount);
//    }
//    else if (statement instanceof Execute) {
//      Execute execute = (Execute) statement;
//      ret = doExecuteProcedure(dbName, sql, execute);
//    }
//    success = true;

  }

  public StatementHandler getHandler(Statement statement) {
    return handlers.get(statement.getClass().getName());
  }

}
