package com.sonicbase.common;

import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.server.SnapshotManager;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Created by lowryda on 4/9/17.
 */
public class Logger {
  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private final DatabaseClient databaseClient;
  private static Thread sendThread;
  private static String hostName;
  private static ArrayBlockingQueue<Error> queue = new ArrayBlockingQueue<Error>(1000);
  private static boolean ready = false;
  private static boolean isClient;

  static {
    try {
      hostName = InetAddress.getLocalHost().getHostName();
      sendThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (true) {
            Error error = null;
            try {
              error = queue.poll(30000, TimeUnit.MILLISECONDS);
              if (error == null) {
                continue;
              }

              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__none__");
              cobj.put(ComObject.Tag.schemaVersion, error.client.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.method, "logError");
              cobj.put(ComObject.Tag.isClient, isClient);
              cobj.put(ComObject.Tag.host, hostName);
              cobj.put(ComObject.Tag.message, error.msg);
              if (error.e != null) {
                String exception = ExceptionUtils.getFullStackTrace(error.e);
                cobj.put(ComObject.Tag.exception, exception);
              }
              String command = "DatabaseServer:ComObject:logError:";
              int masterReplica = error.client.getCommon().getServersConfig().getShards()[0].getMasterReplica();
              byte[] ret = error.client.send(null, 0, masterReplica, command, cobj, DatabaseClient.Replica.specified, true);
            }
            catch (InterruptedException e) {
              break;
            }
            catch (Exception e) {
              try {
                error.client.syncSchema();
              }
              catch (Exception e1) {
                logger.error("Error getting schema", e1);
              }
              logger.error("Error sending error to master: error=" + error.e);
            }
          }
        }
      });
      sendThread.start();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  private int shard = -1;
  private int replica = -1;

  public Logger(final DatabaseClient databaseClient) {
    try {
      this.databaseClient = databaseClient;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public Logger(DatabaseClient databaseClient, int shard, int replica) {
    this.databaseClient = databaseClient;
    this.shard = shard;
    this.replica = replica;
  }

  public static void setReady() {
    ready = true;
  }

  public void errorLocalOnly(String msg, Throwable throwable) {
    logger.error(msg, throwable);
  }

  class Error {
    DatabaseClient client;
    String msg;
    Throwable e;

    public Error(DatabaseClient databaseClient, String msg, Throwable e) {
      this.client = databaseClient;
      this.msg = msg;
      this.e = e;
    }
  }

  public void info(String msg) {
    if (shard != -1) {
      logger.info("shard=" + shard + ", replica=" + replica + " " + msg);
    }
    else {
      logger.info(msg);
    }
  }

  public void warn(String msg) {
    if (shard != -1) {
      logger.warn("shard=" + shard + ", replica=" + replica + " " + msg);
    }
    else {
      logger.warn(msg);
    }
  }

  public void error(String msg, Throwable e) {
    try {
      if (e == null) {
        if (shard != -1) {
          logger.error("shard=" + shard + ", replica=" + replica + " " + msg);
        }
        else {
          logger.error(msg);
        }
      }
      else {
        if (shard != -1) {
          logger.error("shard=" + shard + ", replica=" + replica + " " + msg, e);
        }
        else {
          logger.error(msg, e);
        }
      }
      if (ready) {
        if (shard != -1) {
          queue.put(new Error(databaseClient, "shard=" + shard + ", replica=" + replica + " " + msg, e));
        }
        else {
          queue.put(new Error(databaseClient, msg, e));
        }
      }
    }
    catch (Exception e1) {
      logger.error(msg, e);
    }
  }

  public void error(String msg) {
    error(msg, null);
  }

  public void sendErrorToServer(String msg, Throwable e) {
    try {
      if (ready) {
        if (shard != -1) {
          queue.put(new Error(databaseClient, "shard=" + shard + ", replica=" + replica + " " + msg, e));
        }
        else {
          queue.put(new Error(databaseClient, msg, e));
        }
      }
    }
    catch (InterruptedException e1) {
      throw new DatabaseException(e1);
    }
  }

  public static void setIsClient(boolean isClient) {
    Logger.isClient = isClient;
  }

}
