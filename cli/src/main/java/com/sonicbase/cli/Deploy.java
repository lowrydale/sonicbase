package com.sonicbase.cli;

import com.sonicbase.common.Config;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by lowryda on 3/28/17.
 */
public class Deploy {

  private static final int FAN_OUT = 4;
  public static final String USER_DIR_STR = "user.dir";

  static class Node {
    private String id;
    private String address;
  }

  public static void main(String[] args) {
    final String thisNode = args[0];
    final String gatewayAddress = args[1];
    final String installDir = args[2];
    Deploy deploy = new Deploy();
    deploy.deploy(gatewayAddress, installDir, thisNode);
  }

  private void printException(Exception e) {
    e.printStackTrace();
  }

  void println(String msg) {
    System.out.println(msg);
  }

  void write(int b) {
    System.out.write(b);
  }

  void deploy(String gatewayAddress, String installDir, final String thisNodeId) {
    try {
      List<Node> nodes = new ArrayList<>();
      InputStream in = Config.getConfigStream();
      String json = IOUtils.toString(in, "utf-8");
      Config config = new Config(json);
      final String deployUser = config.getString("user");
      int nodeCount = 0;

      List<Config.Client> clients = config.getClients();
      List<Config.Shard> shards = config.getShards();
      for (int i = 0; i < shards.size(); i++) {
        Config.Shard shard = shards.get(i);
        List<Config.Replica> replicas = shard.getReplicas();
        for (int j = 0; j < replicas.size(); j++) {
          Node node = new Node();
          if (nodeCount == 0) {
            node.address = gatewayAddress;
          }
          else {
            node.address = replicas.get(j).getString("address");
          }
          nodes.add(node);
          nodeCount++;
        }
      }

      for (int i = 0; i < clients.size(); i++) {
        Config.Client client = clients.get(i);
        Node node = new Node();
        node.address = client.getString("address");
        nodes.add(node);
        nodeCount++;
      }

      List<String> stack = new ArrayList<>();
      assignNode("1", nodes, stack);
      while (!stack.isEmpty()) {
        recurse(stack.remove(0), nodes, stack);
      }

      for (Node node : nodes) {
        println("id=" + node.id + ", address=" + node.address);
      }
      if (thisNodeId.equals("0")) {
        deployToAServer(deployUser, nodes.get(0), gatewayAddress, installDir);
      }
      else {
        List<Future> futures = new ArrayList<>();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(FAN_OUT, FAN_OUT, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        try {
          int offset = 1;
          for (final Node node : nodes) {
            println("node.id=" + node.id + ", thisNodeId=" + thisNodeId);
            if (node.id.equals(thisNodeId + "." + offset)) {
              futures.add(executor.submit((Callable) () -> {
                deployToAServer(deployUser, node, gatewayAddress, installDir);

                return null;
              }));
              offset++;
            }
          }
          for (Future future : futures) {
            future.get();
          }
        }
        catch (Exception e) {
          printException(e);
        }
        finally {
          executor.shutdownNow();
        }
      }
    }
    catch (Exception e) {
      printException(e);
    }
  }

  private void deployToAServer(String deployUser, Node node, String gatewayAddress, String installDir) throws IOException, InterruptedException {
    println("Deploying to a server: id=" + node.id + ", address=" + node.address + ", userDir=" + System.getProperty(USER_DIR_STR) + ", command=" + "bin/do-rsync " + deployUser + "@" + node.address + ":" + installDir);
    ProcessBuilder builder = new ProcessBuilder().command("bash", "bin/do-rsync", deployUser + "@" + node.address, installDir);
    Process p = builder.start();
    InputStream in = p.getInputStream();
    while (true) {
      int b = in.read();
      if (b == -1) {
        break;
      }
      write(b);
    }
    p.waitFor();

    builder = new ProcessBuilder().command("bash", "bin/do-deploy", deployUser + "@" + node.address, installDir, node.id, gatewayAddress, installDir);
    p = builder.start();
    in = p.getInputStream();
    while (true) {
      int b = in.read();
      if (b == -1) {
        break;
      }
      write(b);
    }
    in = p.getErrorStream();
    while (true) {
      int b = in.read();
      if (b == -1) {
        break;
      }
      write(b);
    }
    p.waitFor();
  }

  private static void recurse(String id, List<Node> nodes, List<String> stack) {
    for (int i = 1; i < 1 + FAN_OUT; i++) {
      assignNode(id + "." + i, nodes, stack);
    }
  }

  private static boolean assignNode(String id, List<Node> nodes, List<String> stack) {
    int offset = findNext(nodes);
    if (offset == -1) {
      return false;
    }
    nodes.get(offset).id = id;
    stack.add(id);
    return true;
  }

  private static int findNext(List<Node> nodes) {
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i).id == null) {
        return i;
      }
    }
    return -1;
  }
}
