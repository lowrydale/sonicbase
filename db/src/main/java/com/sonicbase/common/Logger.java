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
            try {
              Error error = queue.poll(30000, TimeUnit.MILLISECONDS);
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
              byte[] ret = error.client.sendToMaster(command, cobj.serialize());
            }
            catch (InterruptedException e) {
              break;
            }
            catch (Exception e) {
              logger.error("Error sending error to master", e);
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

  public Logger(final DatabaseClient databaseClient) {
    try {
      this.databaseClient = databaseClient;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
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
    logger.info(msg);
  }

  public void warn(String msg) {
    logger.warn(msg);
  }

  public void error(String msg, Throwable e) {
    try {
      if (e == null) {
        logger.error(msg);
      }
      else {
        logger.error(msg, e);
      }
      if (ready) {
        queue.put(new Error(databaseClient, msg, e));
      }
    }
    catch (InterruptedException e1) {
      throw new DatabaseException(e1);
    }
  }

  public void error(String msg) {
    error(msg, null);
  }

  public void sendErrorToServer(String msg, Throwable e) {
    try {
      if (ready) {
        queue.put(new Error(databaseClient, msg, e));
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
