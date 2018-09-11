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

@SuppressWarnings({"squid:S1168", "squid:S00107"})
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class StatementHandlerFactory {

  private final ConcurrentHashMap<String, StatementHandler> handlers = new ConcurrentHashMap<>();

  public StatementHandlerFactory(DatabaseClient client) {
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
  }

  public StatementHandler getHandler(Statement statement) {
    return handlers.get(statement.getClass().getName());
  }

}
