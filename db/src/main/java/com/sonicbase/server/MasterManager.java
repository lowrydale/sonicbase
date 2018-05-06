/* © 2018 by Intellectual Reserve, Inc. All rights reserved. */
package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MasterManager {

  private static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("com.sonicbase.logger");
  private final DatabaseServer server;
  private Timer fixSchemaTimer;
  private ArrayList<Thread> masterMonitorThreadsForShards;
  private Thread masterMonitorThread;

  public MasterManager(DatabaseServer server) {
    this.server = server;
  }

  public void shutdownFixSchemaTimer() {
    if (fixSchemaTimer != null) {
      fixSchemaTimer.cancel();
      fixSchemaTimer = null;
    }
  }

  public void shutdown() {
    try {
      if (fixSchemaTimer != null) {
        fixSchemaTimer.cancel();
        fixSchemaTimer = null;
      }

      if (masterMonitorThread != null) {
        masterMonitorThread.interrupt();
        masterMonitorThread.join();
      }

      if (masterMonitorThreadsForShards != null) {
        for (Thread thread : masterMonitorThreadsForShards) {
          thread.interrupt();
        }
        for (Thread thread : masterMonitorThreadsForShards) {
          thread.join();
        }
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void startMasterMonitor() {
    if (server.getReplicationFactor() == 1) {
      if (server.getShard() != 0) {
        return;
      }
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!server.getShutdown()) {
            try {
              promoteToMaster(null, false);
              break;
            }
            catch (Exception e) {
              logger.error("Error promoting to master", e);
              try {
                Thread.sleep(2000);
              }
              catch (InterruptedException e1) {
                break;
              }
            }
          }
        }
      });
      thread.start();
      return;
    }

    masterMonitorThreadsForShards = new ArrayList<>();

    masterMonitorThread = ThreadUtil.createThread(new Runnable() {
      @Override
      public void run() {
        final int[] monitorShards = {0, 0, 0};
        final int[] monitorReplicas = {0, 1, 2};

        ArrayNode shards = server.getConfig().withArray("shards");
        ObjectNode replicasNode = (ObjectNode) shards.get(0);
        ArrayNode replicas = replicasNode.withArray("replicas");
        if (replicas.size() < 3) {
          monitorShards[2] = 1;
          monitorReplicas[2] = 0;
        }
        boolean shouldMonitor = false;
        if (server.getShard() == 0 && (server.getReplica() == 0 || server.getReplica() == 1 || server.getReplica() == 3)) {
          shouldMonitor = true;
        }
        else if (replicas.size() < 3 && server.getShard() == 1 && server.getReplica()== 0) {
          shouldMonitor = true;
        }
        if (shouldMonitor) {

          for (int i = 0; i < server.getShardCount(); i++) {
            final int shard = i;
            Thread masterThreadForShard = ThreadUtil.createThread(new Runnable() {
              @Override
              public void run() {
                AtomicInteger nextMonitor = new AtomicInteger(-1);
                while (!server.getShutdown() && nextMonitor.get() == -1) {
                  try {
                    Thread.sleep(2000);
                    electNewMaster(shard, -1, monitorShards, monitorReplicas, nextMonitor);
                  }
                  catch (Exception e) {
                    logger.error("Error electing master: shard=" + shard, e);
                  }
                }
                while (!server.getShutdown()) {
                  try {
                    Thread.sleep(server.getDeathOverride() == null ? 2000 : 1000);

                    final int masterReplica = server.getCommon().getServersConfig().getShards()[shard].getMasterReplica();
                    if (masterReplica == -1) {
                      electNewMaster(shard, masterReplica, monitorShards, monitorReplicas, nextMonitor);
                    }
                    else {
                      final AtomicBoolean isHealthy = new AtomicBoolean(false);
                      for (int i = 0; i < 5; i++) {
                        server.checkHealthOfServer(shard, masterReplica, isHealthy, true);
                        if (isHealthy.get()) {
                          break;
                        }
                      }

                      if (!isHealthy.get()) {
                        electNewMaster(shard, masterReplica, monitorShards, monitorReplicas, nextMonitor);
                      }
                    }
                  }
                  catch (Exception e) {
                    logger.error("Error in master monitor: shard=" + shard, e);
                  }
                }
              }
            }, "SonicBase Master Monitor Thread for Shard: " + shard);
            masterThreadForShard.start();
            masterMonitorThreadsForShards.add(masterThreadForShard);
          }
        }
      }
    }, "SonicBase Maste Monitor Thread");
    masterMonitorThread.start();
  }

  private boolean electNewMaster(int shard, int oldMasterReplica, int[] monitorShards, int[] monitorReplicas,
                                 AtomicInteger nextMonitor) throws InterruptedException, IOException {
    int electedMaster = -1;
    boolean isFirst = false;
    nextMonitor.set(-1);
    logger.info("electNewMaster - begin: shard=" + shard + ", oldMasterReplica=" + oldMasterReplica);
    for (int i = 0; i < monitorShards.length; i++) {
      if (monitorShards[i] == server.getShard() && monitorReplicas[i] == server.getReplica()) {
        isFirst = true;
        continue;
      }
      final AtomicBoolean isHealthy = new AtomicBoolean(false);
      server.checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy, true);
      if (!isHealthy.get()) {
        continue;
      }
      nextMonitor.set(i);
      break;
//      // let the lowest monitor initiate the election
//      if (monitorShards[i] != 0 || monitorReplicas[i] != oldMasterReplica) {
//        if (monitorShards[i] != shard || monitorReplicas[i] != replica) {
//          isFirst = false;
//          break;
//        }
//        nextMonitor = i;
//        break;
//      }
    }
    if (!isFirst) {
      logger.info("ElectNewMaster shard=" + shard + ", !isFirst, nextMonitor=" + nextMonitor);
      return isFirst;
    }
    if (nextMonitor.get() == -1) {
      logger.error("ElectNewMaster shard=" + shard + ", isFirst, nextMonitor==-1");
      Thread.sleep(5000);
    }
    else {
      logger.error("ElectNewMaster, shard=" + shard + ", checking candidates");
      outer:
      while (!server.getShutdown()) {
        for (int j = 0; j < server.getReplicationFactor(); j++) {
          if (j == oldMasterReplica) {
            continue;
          }
          Thread.sleep(server.getDeathOverride() == null ? 2000 : 50);

          AtomicBoolean isHealthy = new AtomicBoolean();
          if (shard == server.getShard() && j == server.getReplica()) {
            isHealthy.set(true);
          }
          else {
            server.checkHealthOfServer(shard, j, isHealthy, false);
          }
          if (isHealthy.get()) {
            try {
              logger.info("ElectNewMaster, electing new: nextMonitor=" + nextMonitor.get() + ", shard=" + shard + ", replica=" + j);
              ComObject cobj = new ComObject();
              cobj.put(ComObject.Tag.dbName, "__non__");
              cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
              cobj.put(ComObject.Tag.method, "MasterManager:electNewMaster");
              cobj.put(ComObject.Tag.requestedMasterShard, shard);
              cobj.put(ComObject.Tag.requestedMasterReplica, j);
              byte[] bytes = server.getDatabaseClient().send(null, monitorShards[nextMonitor.get()], monitorReplicas[nextMonitor.get()],
                  cobj, DatabaseClient.Replica.specified);
              ComObject retObj = new ComObject(bytes);
              int otherServersElectedMaster = retObj.getInt(ComObject.Tag.selectedMasteReplica);
              if (otherServersElectedMaster != j) {
                logger.info("Other server elected different master: shard=" + shard + ", other=" + otherServersElectedMaster);
                Thread.sleep(2000);
                continue;
              }
              else {
                logger.info("Other server elected same master: shard=" + shard + ", master=" + otherServersElectedMaster);
                electedMaster = otherServersElectedMaster;
                break;
              }
            }
            catch (Exception e) {
              logger.error("Error electing new master: shard=" + shard, e);
              Thread.sleep(2000);
              continue;
            }
          }
        }
        try {
          if (electedMaster != -1) {
            logger.info("Elected master: shard=" + shard + ", replica=" + electedMaster);
            ComObject cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "DatabaseServer:promoteToMasterAndPushSchema");
            cobj.put(ComObject.Tag.shard, shard);
            cobj.put(ComObject.Tag.replica, electedMaster);
            server.getCommon().getServersConfig().getShards()[shard].setMasterReplica(electedMaster);

            server.getDatabaseClient().sendToMaster(cobj);


            cobj = new ComObject();
            cobj.put(ComObject.Tag.dbName, "__none__");
            cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
            cobj.put(ComObject.Tag.method, "MasterManager:promoteToMaster");
            cobj.put(ComObject.Tag.shard, shard);
            cobj.put(ComObject.Tag.electedMaster, electedMaster);

            server.getDatabaseClient().send(null, shard, electedMaster, cobj, DatabaseClient.Replica.specified);
            return true;
          }
        }
        catch (Exception e) {
          logger.error("Error promoting master: shard=" + shard + ", electedMaster=" + electedMaster, e);
          continue;
        }
      }
    }
    return false;
  }

  public ComObject promoteEntireReplicaToMaster(ComObject cobj, boolean replayedCommand) {
    final int newReplica = cobj.getInt(ComObject.Tag.replica);
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      logger.info("promoting to master: shard=" + shard + ", replica=" + newReplica);
      server.getCommon().getServersConfig().getShards()[shard].setMasterReplica(newReplica);
    }

    server.getCommon().saveSchema(server.getClient(), server.getDataDir());
    server.pushSchema();

    List<Future> futures = new ArrayList<>();
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      final int localShard = shard;
      futures.add(server.getExecutor().submit(new Callable() {
        @Override
        public Object call() throws Exception {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "MasterManager:promoteToMaster");
          cobj.put(ComObject.Tag.shard, localShard);
          cobj.put(ComObject.Tag.electedMaster, newReplica);

          server.getDatabaseClient().send(null, localShard, newReplica, cobj, DatabaseClient.Replica.specified);
          return null;
        }
      }));
    }

    for (Future future : futures) {
      try {
        future.get();
      }
      catch (Exception e) {
        throw new DatabaseException(e);
      }
    }

    return null;
  }

  public ComObject electNewMaster(ComObject cobj, boolean replayedCommand) throws InterruptedException, IOException {
    int requestedMasterShard = cobj.getInt(ComObject.Tag.requestedMasterShard);
    int requestedMasterReplica = cobj.getInt(ComObject.Tag.requestedMasterReplica);

    final AtomicBoolean isHealthy = new AtomicBoolean(false);
    server.checkHealthOfServer(requestedMasterShard, requestedMasterReplica, isHealthy, false);
    if (!isHealthy.get()) {
      logger.info("candidate master is unhealthy, rejecting: shard=" + requestedMasterShard + ", replica=" + requestedMasterReplica);
      requestedMasterReplica = -1;
    }
    else {
      logger.info("candidate master is healthy, accepting: shard=" + requestedMasterShard + ", candidateMaster=" + requestedMasterReplica);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.selectedMasteReplica, requestedMasterReplica);
    return retObj;
  }

  public boolean isNoLongerMaster() throws InterruptedException {

    final int[] monitorShards = {0, 0, 0};
    final int[] monitorReplicas = {0, 1, 2};

    ArrayNode shards = server.getConfig().withArray("shards");
    ArrayNode replicas = (ArrayNode) shards.get(0).withArray("replicas");
    if (replicas.size() < 3) {
      monitorShards[2] = 1;
      monitorReplicas[2] = 0;
    }

    int countAgree = 0;
    int countDisagree = 0;
    for (int i = 0; i < monitorShards.length; i++) {
      try {
        AtomicBoolean isHealthy = new AtomicBoolean();
        server.checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy, true);

        if (isHealthy.get()) {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.dbName, "__none__");
          cobj.put(ComObject.Tag.schemaVersion, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.method, "DatabaseServer:getSchema");

          byte[] ret = server.getClient().send(null, monitorShards[i], monitorReplicas[i], cobj, DatabaseClient.Replica.specified, true);
          DatabaseCommon tempCommon = new DatabaseCommon();
          ComObject retObj = new ComObject(ret);
          tempCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.schemaBytes));
          int masterReplica = tempCommon.getServersConfig().getShards()[0].getMasterReplica();
          if (masterReplica == server.getReplica()) {
            countAgree++;
          }
          else {
            countDisagree++;
          }
        }
      }
      catch (InterruptedException e) {
        throw e;
      }
      catch (Exception e) {
        if (!server.getShutdownDeathMonitor()) {
          logger.error("Error checking if master", e);
        }
      }
    }
    if (countAgree < countDisagree) {
      return true;
    }
    return false;
  }

  public ComObject promoteToMaster(ComObject cobj, boolean replayedCommand) {
    try {
      logger.info("Promoting to master: shard=" + server.getShard() + ", replica=" + server.getReplica());
      server.getLogManager().skipToMaxSequenceNumber();

      if (server.getShard() == 0) {
        server.getLicenseManager().shutdownMasterLicenseValidator();
        server.getLicenseManager().startMasterLicenseValidator();

        fixSchemaTimer = new Timer();
        fixSchemaTimer.schedule(new TimerTask(){
          @Override
          public void run() {
            server.getSchemaManager().reconcileSchema();
          }
        }, 5 * 60 * 1000, 5 * 60 * 1000);

        server.getMonitorManager().startMasterMonitor();

        server.startStreamsConsumerMonitor();

        server.shutdownDeathMonitor();

        logger.info("starting death monitor");
        server.startDeathMonitor();

        logger.info("starting repartitioner");
        server.startRepartitioner();
      }
      return null;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

}
