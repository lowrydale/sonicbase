package com.sonicbase.client;

import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.procedure.StoredProcedureContextImpl;
import com.sonicbase.query.DatabaseException;

import java.lang.reflect.Method;

public class DatabaseServerProxy {

  private static Method indexLookupExpression;
  private static Method invokeMethod;
  private static Method serverSelect;
  private static Method indexLookup;
  private static Method serverSetSelect;
  private static Method getShard;
  private static Method getReplica;
  private static Method getCommon;

  static {
    try {
      Class clz = Class.forName("com.sonicbase.server.DatabaseServer");
      indexLookupExpression = clz.getMethod("indexLookupExpression", ComObject.class,  StoredProcedureContextImpl.class);
      invokeMethod = clz.getMethod("invokeMethod", byte[].class, boolean.class, boolean.class);
      serverSelect = clz.getMethod("serverSelect", ComObject.class, boolean.class, StoredProcedureContextImpl.class);
      indexLookup = clz.getMethod("indexLookup", ComObject.class, StoredProcedureContextImpl.class);
      serverSetSelect = clz.getMethod("serverSetSelect", ComObject.class, boolean.class, StoredProcedureContextImpl.class);
      getShard = clz.getMethod("getShard");
      getReplica = clz.getMethod("getReplica");
      getCommon = clz.getMethod("getCommon");
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private DatabaseServerProxy() {

  }

  public static ComObject serverSelect(Object server, ComObject cobj, boolean restrictToThisServer, StoredProcedureContextImpl procedureContext) {
    try {
      return (ComObject) serverSelect.invoke(server, cobj, restrictToThisServer, procedureContext);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static byte[] invokeMethod(Object server, final byte[] body, boolean replayedCommand, boolean enableQueuing) {
    try {
      return (byte[]) invokeMethod.invoke(server, body, replayedCommand, enableQueuing);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static ComObject indexLookupExpression(Object server, ComObject cobj, StoredProcedureContextImpl context) {
    try {
      return (ComObject) indexLookupExpression.invoke(server, cobj, context);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static ComObject indexLookup(Object server, ComObject cobj, StoredProcedureContextImpl procedureContext) {
    try {
      return (ComObject) indexLookup.invoke(server, cobj, procedureContext);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static ComObject serverSetSelect(Object server, ComObject cobj, final boolean restrictToThisServer, final StoredProcedureContextImpl procedureContext) {
    try {
      return (ComObject) serverSetSelect.invoke(server, cobj, restrictToThisServer, procedureContext);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static int getShard(Object server) {
    try {
      return (int) getShard.invoke(server);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static int getReplica(Object server) {
    try {
      return (int) getReplica.invoke(server);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public static DatabaseCommon getCommon(Object server) {
    try {
      return (DatabaseCommon) getCommon.invoke(server);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
