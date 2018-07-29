package com.sonicbase.server;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.ComObject;
import com.sonicbase.common.DatabaseCommon;
import com.sonicbase.common.ThreadUtil;
import com.sonicbase.query.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"squid:S1172", "squid:S1168", "squid:S00107"})
// all methods called from method invoker must have cobj and replayed command parms
// I prefer to return null instead of an empty array
// I don't know a good way to reduce the parameter count
public class MasterManager {

  public static final String NONE_STR = "__none__";
  private static Logger logger = LoggerFactory.getLogger(MasterManager.class);

  private final com.sonicbase.server.DatabaseServer server;
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
    startMasterMonitor(null);
  }

  public void startMasterMonitor(Long timeoutOverride) {
    if (server.getReplicationFactor() == 1) {
      if (server.getShard() != 0) {
        return;
      }
      promoteToMaster(timeoutOverride);
      return;
    }

    masterMonitorThreadsForShards = new ArrayList<>();

    masterMonitorThread = ThreadUtil.createThread(() -> masterMonitorThread(timeoutOverride), "SonicBase Maste Monitor Thread");
    masterMonitorThread.start();
  }

  private void masterMonitorThread(Long timeoutOverride) {
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
    if (server.getShard() == 0 && (server.getReplica() == 0 || server.getReplica() == 1 || server.getReplica() == 3) ||
        (replicas.size() < 3 && server.getShard() == 1 && server.getReplica() == 0)) {
      shouldMonitor = true;
    }
    if (shouldMonitor) {
      for (int i = 0; i < server.getShardCount(); i++) {
        final int shard = i;
        Thread masterThreadForShard = ThreadUtil.createThread(() -> masterMonitorThreadForShard(timeoutOverride,
            monitorShards, monitorReplicas, shard), "SonicBase Master Monitor Thread for Shard: " + shard);
        masterThreadForShard.start();
        masterMonitorThreadsForShards.add(masterThreadForShard);
      }
    }
  }

  private void masterMonitorThreadForShard(Long timeoutOverride, int[] monitorShards, int[] monitorReplicas, int shard) {
    AtomicInteger nextMonitor = new AtomicInteger(-1);
    while (!server.getShutdown() && nextMonitor.get() == -1) {
      try {
        Thread.sleep(timeoutOverride == null ? 2000 : timeoutOverride);
        electNewMaster(shard, -1, monitorShards, monitorReplicas, nextMonitor);
      }
      catch (Exception e) {
        logger.error("Error electing master: shard=" + shard, e);
      }
    }
    electNewMasterIfNeeded(timeoutOverride, monitorShards, monitorReplicas, shard, nextMonitor);
  }

  private void electNewMasterIfNeeded(Long timeoutOverride, int[] monitorShards, int[] monitorReplicas, int shard,
                                      AtomicInteger nextMonitor) {
    while (!server.getShutdown()) {
      try {
        long timeout = DatabaseServer.getDeathOverride() == null ? 2000 : 1000;
        Thread.sleep(timeoutOverride == null ? timeout : timeoutOverride);

        final int masterReplica = server.getCommon().getServersConfig().getShards()[shard].getMasterReplica();
        if (masterReplica == -1) {
          electNewMaster(shard, masterReplica, monitorShards, monitorReplicas, nextMonitor);
        }
        else {
          final AtomicBoolean isHealthy = doCheckHealthOfServer(shard, masterReplica);

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

  private AtomicBoolean doCheckHealthOfServer(int shard, int masterReplica) throws InterruptedException {
    final AtomicBoolean isHealthy = new AtomicBoolean(false);
    for (int i1 = 0; i1 < 5; i1++) {
      server.checkHealthOfServer(shard, masterReplica, isHealthy);
      if (isHealthy.get()) {
        break;
      }
    }
    return isHealthy;
  }

  private void promoteToMaster(Long timeoutOverride) {
    Thread thread = new Thread(() -> {
      while (!server.getShutdown()) {
        try {
          promoteToMaster(null, false);
          break;
        }
        catch (Exception e) {
          logger.error("Error promoting to master", e);
          try {
            Thread.sleep(timeoutOverride == null ? 2000 : timeoutOverride);
          }
          catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    });
    thread.start();
  }

  private boolean electNewMaster(int shard, int oldMasterReplica, int[] monitorShards, int[] monitorReplicas,
                                 AtomicInteger nextMonitor) throws InterruptedException {
    int electedMaster = -1;
    nextMonitor.set(-1);
    logger.info("electNewMaster - begin: shard={}, oldMasterReplica={}", shard, oldMasterReplica);
    boolean isFirst = getNextMonitor(monitorShards, monitorReplicas, nextMonitor);
    if (!isFirst) {
      logger.info("ElectNewMaster shard={}, !isFirst, nextMonitor={}", shard, nextMonitor);
      return isFirst;
    }
    if (nextMonitor.get() == -1) {
      logger.error("ElectNewMaster shard={}, isFirst, nextMonitor==-1", shard);
      Thread.sleep(5000);
    }
    else {
      if (doElectMasterAmongCandidates(shard, oldMasterReplica, monitorShards, monitorReplicas, nextMonitor, electedMaster)) {
        return true;
      }
    }
    return false;
  }

  private boolean doElectMasterAmongCandidates(int shard, int oldMasterReplica, int[] monitorShards,
                                               int[] monitorReplicas, AtomicInteger nextMonitor,
                                               int electedMaster) throws InterruptedException {
    logger.error("ElectNewMaster, shard={}, checking candidates", shard);
    while (!server.getShutdown()) {
      for (int j = 0; j < server.getReplicationFactor(); j++) {
        if (j == oldMasterReplica) {
          continue;
        }
        Thread.sleep(DatabaseServer.getDeathOverride() == null ? 2000 : 50);

        DoElectNewMaster doElectNewMaster = new DoElectNewMaster(shard, monitorShards[nextMonitor.get()],
            monitorReplicas[nextMonitor.get()], nextMonitor, electedMaster, j).invoke();
        electedMaster = doElectNewMaster.getElectedMaster();
        if (doElectNewMaster.is()) {
          break;
        }
      }
      if (promoteToMaster(shard, electedMaster)) {
        return true;
      }
    }
    return false;
  }

  private boolean promoteToMaster(int shard, int electedMaster) {
    try {
      if (electedMaster != -1) {
        logger.info("Elected master: shard={}, replica={}", shard, electedMaster);
        ComObject cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.SHARD, shard);
        cobj.put(ComObject.Tag.REPLICA, electedMaster);
        server.getCommon().getServersConfig().getShards()[shard].setMasterReplica(electedMaster);

        server.getDatabaseClient().sendToMaster("DatabaseServer:promoteToMasterAndPushSchema", cobj);

        cobj = new ComObject();
        cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
        cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        cobj.put(ComObject.Tag.SHARD, shard);
        cobj.put(ComObject.Tag.ELECTED_MASTER, electedMaster);

        server.getDatabaseClient().send("MasterManager:promoteToMaster", shard, electedMaster, cobj,
            DatabaseClient.Replica.SPECIFIED);
        return true;
      }
    }
    catch (Exception e) {
      logger.error("Error promoting master: shard=" + shard + ", electedMaster=" + electedMaster, e);
    }
    return false;
  }

  private boolean getNextMonitor(int[] monitorShards, int[] monitorReplicas,
                                 AtomicInteger nextMonitor) throws InterruptedException {
    boolean isFirst = false;
    for (int i = 0; i < monitorShards.length; i++) {
      if (monitorShards[i] == server.getShard() && monitorReplicas[i] == server.getReplica()) {
        isFirst = true;
        continue;
      }
      final AtomicBoolean isHealthy = new AtomicBoolean(false);
      server.checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy);
      if (!isHealthy.get()) {
        continue;
      }
      nextMonitor.set(i);
      break;
    }
    return isFirst;
  }

  public ComObject promoteEntireReplicaToMaster(ComObject cobj, boolean replayedCommand) {
    final int newReplica = cobj.getInt(ComObject.Tag.REPLICA);
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      logger.info("promoting to master: shard={}, replica={}", shard, newReplica);
      server.getCommon().getServersConfig().getShards()[shard].setMasterReplica(newReplica);
    }

    server.getCommon().saveSchema(server.getDataDir());
    server.pushSchema();

    List<Future> futures = new ArrayList<>();
    for (int shard = 0; shard < server.getShardCount(); shard++) {
      final int localShard = shard;
      futures.add(server.getExecutor().submit((Callable) () -> {
        ComObject cobj1 = new ComObject();
        cobj1.put(ComObject.Tag.DB_NAME, NONE_STR);
        cobj1.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
        cobj1.put(ComObject.Tag.METHOD, "MasterManager:promoteToMaster");
        cobj1.put(ComObject.Tag.SHARD, localShard);
        cobj1.put(ComObject.Tag.ELECTED_MASTER, newReplica);

        server.getDatabaseClient().send(null, localShard, newReplica, cobj1, DatabaseClient.Replica.SPECIFIED);
        return null;
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

  public ComObject electNewMaster(ComObject cobj, boolean replayedCommand) throws InterruptedException {
    int requestedMasterShard = cobj.getInt(ComObject.Tag.REQUESTED_MASTER_SHARD);
    int requestedMasterReplica = cobj.getInt(ComObject.Tag.REQUESTED_MASTER_REPLICA);

    final AtomicBoolean isHealthy = new AtomicBoolean(false);
    server.checkHealthOfServer(requestedMasterShard, requestedMasterReplica, isHealthy);
    if (!isHealthy.get()) {
      logger.info("candidate master is unhealthy, rejecting: shard={}, replica={}", requestedMasterShard, requestedMasterReplica);
      requestedMasterReplica = -1;
    }
    else {
      logger.info("candidate master is healthy, accepting: shard={}, candidateMaster={}", requestedMasterShard, requestedMasterReplica);
    }
    ComObject retObj = new ComObject();
    retObj.put(ComObject.Tag.SELECTED_MASTE_REPLICA, requestedMasterReplica);
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
        server.checkHealthOfServer(monitorShards[i], monitorReplicas[i], isHealthy);

        if (isHealthy.get()) {
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, NONE_STR);
          cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.METHOD, "DatabaseServer:getSchema");

          byte[] ret = server.getClient().send(null, monitorShards[i], monitorReplicas[i], cobj,
              DatabaseClient.Replica.SPECIFIED, true);
          DatabaseCommon tempCommon = new DatabaseCommon();
          ComObject retObj = new ComObject(ret);
          tempCommon.deserializeSchema(retObj.getByteArray(ComObject.Tag.SCHEMA_BYTES));
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
    return countAgree < countDisagree;
  }

  public ComObject promoteToMaster(ComObject cobj, boolean replayedCommand) {
    try {
      logger.info("Promoting to master: shard={}, replica={}", server.getShard(), server.getReplica());
      server.getLogManager().skipToMaxSequenceNumber();

      if (server.getShard() == 0) {

        fixSchemaTimer = new Timer();
        fixSchemaTimer.schedule(new TimerTask(){
          @Override
          public void run() {
            server.getSchemaManager().reconcileSchema();
          }
        }, 5 * 60 * 1000L, 5 * 60 * 1000L);

        server.startStreamsConsumerMonitor();
        server.startMasterLicenseValidator();

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

  private class DoElectNewMaster {
    private boolean myResult;
    private int shard;
    private int monitorShard;
    private int monitorReplica;
    private AtomicInteger nextMonitor;
    private int electedMaster;
    private int j;

    public DoElectNewMaster(int shard, int monitorShard, int monitorReplica, AtomicInteger nextMonitor, int electedMaster, int j) {
      this.shard = shard;
      this.monitorShard = monitorShard;
      this.monitorReplica = monitorReplica;
      this.nextMonitor = nextMonitor;
      this.electedMaster = electedMaster;
      this.j = j;
    }

    boolean is() {
      return myResult;
    }

    public int getElectedMaster() {
      return electedMaster;
    }

    private AtomicBoolean doGetHealthOfMasterCandidate(int shard, int j) throws InterruptedException {
      AtomicBoolean isHealthy = new AtomicBoolean();
      if (shard == server.getShard() && j == server.getReplica()) {
        isHealthy.set(true);
      }
      else {
        server.checkHealthOfServer(shard, j, isHealthy);
      }
      return isHealthy;
    }

    public DoElectNewMaster invoke() throws InterruptedException {
      try {
        AtomicBoolean isHealthy = doGetHealthOfMasterCandidate(shard, j);
        if (isHealthy.get()) {

          logger.info("ElectNewMaster, electing new: nextMonitor={}, shard={}, replica={}",
              nextMonitor.get(), shard, j);
          ComObject cobj = new ComObject();
          cobj.put(ComObject.Tag.DB_NAME, "__non__");
          cobj.put(ComObject.Tag.SCHEMA_VERSION, server.getCommon().getSchemaVersion());
          cobj.put(ComObject.Tag.METHOD, "MasterManager:electNewMaster");
          cobj.put(ComObject.Tag.REQUESTED_MASTER_SHARD, shard);
          cobj.put(ComObject.Tag.REQUESTED_MASTER_REPLICA, j);
          byte[] bytes = server.getDatabaseClient().send(null, monitorShard, monitorReplica,
              cobj, DatabaseClient.Replica.SPECIFIED);
          ComObject retObj = new ComObject(bytes);
          int otherServersElectedMaster = retObj.getInt(ComObject.Tag.SELECTED_MASTE_REPLICA);
          if (otherServersElectedMaster != j) {
            logger.info("Other server elected different master: shard={}, other={}", shard, otherServersElectedMaster);
            Thread.sleep(2000);
          }
          else {
            logger.info("Other server elected same master: shard={}, master={}", shard, otherServersElectedMaster);
            electedMaster = otherServersElectedMaster;
            myResult = true;
            return this;
          }
        }
      }
      catch (Exception e) {
        logger.error("Error electing new master: shard=" + shard, e);
        Thread.sleep(2000);
      }
      myResult = false;
      return this;
    }
  }
}
