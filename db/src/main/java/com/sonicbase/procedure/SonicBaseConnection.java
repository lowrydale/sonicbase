package com.sonicbase.procedure;

import java.sql.Connection;
import java.sql.SQLException;

public interface SonicBaseConnection extends Connection {

  SonicBasePreparedStatement prepareSonicBaseStatement(StoredProcedureContext context, String sql) throws SQLException;

  SonicBasePreparedStatement prepareSonicBaseStatement(StoredProcedureContext context, String sql, int resultSetType, int resultSetConcurrency) throws SQLException;
}
