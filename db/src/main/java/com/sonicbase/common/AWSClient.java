package com.sonicbase.common;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.common.Logger;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by lowryda on 6/16/17.
 */
public class AWSClient {

  private final DatabaseClient client;
  private final Logger logger;
  private File installDir;
  private TransferManager transferManager;

  public AWSClient(DatabaseClient client) {
    this.client = client;
    this.logger = new Logger(client);

  }

  private ThreadPoolExecutor executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
      Runtime.getRuntime().availableProcessors(), 10000, TimeUnit.MILLISECONDS,
      new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

  public TransferManager getTransferManager() {
    synchronized (this) {
      if (transferManager != null) {
        return transferManager;
      }
    }
    File installDir = getInstallDir();
    String cluster = client.getCluster();
    File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    BasicAWSCredentials awsCredentials = null;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

      synchronized (this) {
        this.transferManager = new TransferManager(awsCredentials);
        return this.transferManager;
      }
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private ObjectNode getConfig() {
    try {
      String cluster = client.getCluster();
      File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
      }
      String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      return (ObjectNode) mapper.readTree(configStr);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public File getInstallDir() {
    if (installDir == null) {
      ObjectNode config = getConfig();
      String dir = config.get("installDirectory").asText();
      installDir = new File(dir.replace("$HOME", System.getProperty("user.home")));
    }
    return installDir;
  }

  public AmazonS3 getS3Client() {
    File installDir = getInstallDir();
    String cluster = client.getCluster();
    File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

      ClientConfiguration config = new ClientConfiguration();
      config.setConnectionTimeout(60_000);
      config.setSocketTimeout(6_000_000);
      config.setRequestTimeout(6_000_000);

      return new AmazonS3Client(awsCredentials, config);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public AmazonSQSClient getSQSClient() {
    final ClientConfiguration config = new ClientConfiguration();
    config.setMaxConnections(10);
    config.setRequestTimeout(20_000);
    config.setConnectionTimeout(60_000);

    File installDir = getInstallDir();
    String cluster = client.getCluster();
    File keysFile = new File(installDir, "/keys/" + cluster + "-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
      return new AmazonSQSClient(awsCredentials, config);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  public void deleteDirectory(String bucket, String prefix) {
    AmazonS3 s3client = getS3Client();
    try {
      final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
      ListObjectsV2Result result;
      do {
        result = s3client.listObjectsV2(req);

        for (S3ObjectSummary objectSummary :
            result.getObjectSummaries()) {
          String key = objectSummary.getKey();

          s3client.deleteObject(bucket, key);
        }
        req.setContinuationToken(result.getNextContinuationToken());
      }
      while (result.isTruncated() == true);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void uploadDirectory(final String bucket, final String prefix, final String path,
                              final File srcDir) {
      TransferManager transferManager = getTransferManager();
        MultipleFileUpload xfer = transferManager.uploadDirectory(bucket, prefix + "/" + path, srcDir, true);
    try {
      xfer.waitForCompletion();
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }
    if (false) {
          List<Future> futures = new ArrayList<>();
          File[] files = srcDir.listFiles();
          if (files != null) {
            for (final File file : files) {
              if (file.isDirectory()) {
                uploadDirectory(bucket, prefix, path + "/" + file.getName(), file);
              }
              else {
                futures.add(executor.submit(new Callable() {
                  @Override
                  public Object call() throws Exception {
                    for (int i = 0; i < 10; i++) {
                      try {
                        uploadFile(bucket, prefix, path, file);
                        break;
                      }
                      catch (Exception e) {
                        logger.error("Error uploading file: srcDir=" + file.getAbsolutePath(), e);
                        if (i == 9) {
                          throw new DatabaseException(e);
                        }
                        try {
                          Thread.sleep(2000);
                        }
                        catch (InterruptedException e1) {
                          throw new DatabaseException(e1);
                        }
                      }
                    }
                    return null;
                  }
                }));
              }
            }
          }
          for (Future future : futures) {
            try {
              future.get();
            }
            catch (Exception e) {
              throw new DatabaseException(e);
            }
          }
//      finally {
//        transferManager.shutdownNow();
//      }

        }
  }

  public void uploadFile(String bucket, String prefix, final String path, final File srcFile) {
    AmazonS3 s3client = getS3Client();
    for (int i = 0; i < 10; i++) {
      try {
        s3client.putObject(new PutObjectRequest(
            bucket, prefix + "/" + path + "/" + srcFile.getName(), srcFile));
        break;
      }
      catch (Exception e) {
        logger.error("Error uploading file: srcFile=" + srcFile.getAbsolutePath(), e);
        if (i == 9) {
          throw new DatabaseException(e);
        }
        try {
          Thread.sleep(2000);
        }
        catch (InterruptedException e1) {
          throw new DatabaseException(e1);
        }
      }
    }
  }

  public void downloadFile(String bucket, String prefix, String path, File destFile) {
    AmazonS3 s3client = getS3Client();
    try {
      S3Object object = s3client.getObject(
          new GetObjectRequest(bucket, prefix + "/" + path + "/" + destFile.getName()));
      destFile.getParentFile().mkdirs();
      try (InputStream objectData = object.getObjectContent();
           BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(destFile))) {
        IOUtils.copy(objectData, out);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void downloadFile(String bucket, String key, File destFile) {
    AmazonS3 s3client = getS3Client();
    try {
      destFile.getParentFile().mkdirs();
//      TransferManager manager = getTransferManager();
//      Download download = manager.download(bucket, key, destFile);
//      download.waitForCompletion();
      S3Object object = s3client.getObject(
          new GetObjectRequest(bucket, key));
      try (InputStream objectData = object.getObjectContent();
           BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(destFile))) {
        IOUtils.copy(objectData, out);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public byte[] downloadBytes(String bucket, String key) {
    AmazonS3 s3client = getS3Client();
    try {
      S3Object object = s3client.getObject(
          new GetObjectRequest(bucket, key));
      try (InputStream objectData = object.getObjectContent();
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        IOUtils.copy(objectData, out);
        out.close();
        return out.toByteArray();
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void downloadDirectory(final String bucket, String prefix, String subDirectory, File destDir) {
    TransferManager transferManager = getTransferManager();
    MultipleFileDownload download = transferManager.downloadDirectory(bucket, prefix + "/" + subDirectory, destDir, true);
    try {
      download.waitForCompletion();
    }
    catch (InterruptedException e) {
      throw new DatabaseException(e);
    }

    File srcDir = new File(destDir, prefix + "/" + subDirectory);
    File[] srcFiles = srcDir.listFiles();
    if (srcFiles != null) {
      for (File srcFile : srcFiles) {
        srcFile.renameTo(new File(destDir, srcFile.getName()));
      }
    }
    try {
      int pos = prefix.indexOf("/");
      if (pos != -1) {
        prefix = prefix.substring(0, pos);
      }
      FileUtils.deleteDirectory(new File(destDir, prefix));
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }

    if (false) {
  AmazonS3 s3client = getS3Client();
  if (prefix.charAt(0) == '/') {
    prefix = prefix.substring(1);
  }
  List<Future> futures = new ArrayList<>();
  try {
    final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix + "/" + subDirectory);
    ListObjectsV2Result result;
    do {
      result = s3client.listObjectsV2(req);

      for (S3ObjectSummary objectSummary :
          result.getObjectSummaries()) {
        String key = objectSummary.getKey();

        if (key.charAt(0) == '/') {
          key = key.substring(1);
        }
        final String finalKey = key;
        final File destFile = new File(destDir, key.substring((prefix + "/" + subDirectory).length()));
        destFile.getParentFile().mkdirs();
        futures.add(executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              for (int i = 0; i < 10; i++) {
                try {
                  downloadFile(bucket, finalKey, destFile);
                  break;
                }
                catch (Exception e) {
                  logger.error("Error downloading file: key=" + finalKey, e);
                  if (i == 9) {
                    throw new DatabaseException(e);
                  }
                  Thread.sleep(2000);
                }
              }
            }
            catch (Exception e) {
              logger.error("Error downloading file", e);
            }
          }
        }));

      }
      req.setContinuationToken(result.getNextContinuationToken());
    }
    while (result.isTruncated() == true);
  }
  catch (Exception e) {
    throw new DatabaseException(e);
  }

  for (Future future : futures) {
    try {
      future.get();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
//    try {
//      MultipleFileDownload xfer = transferManager.downloadDirectory(
//          bucket, prefix + "/" + subDirectory, destDir);
//      // loop with Transfer.isDone()
//      File tmpDir = new File(destDir, prefix + "/" + subDirectory);
//      File[] files = tmpDir.listFiles();
//      if (files != null) {
//        for (File file : files) {
//          file.renameTo(new File(destDir, file.getName()));
//        }
//      }
//      xfer.waitForCompletion();
//      int pos = prefix.indexOf("/", 1);
//      if (pos != -1) {
//        prefix = prefix.substring(0, pos);
//      }
//      File dir = new File(destDir, prefix);
//      FileUtils.deleteDirectory(dir);
//    }
//    catch (Exception e) {
//      throw new DatabaseException(e);
//    }
//    finally {
//      transferManager.shutdownNow();
//    }
  }

  public List<String> listDirectSubdirectories(String bucket, String prefix) {
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }

    Set<String> dirs = new HashSet<>();
    AmazonS3 s3client = getS3Client();
    try {
      final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix);
      ListObjectsV2Result result;
      do {
        result = s3client.listObjectsV2(req);

        for (S3ObjectSummary objectSummary :
            result.getObjectSummaries()) {
          String key = objectSummary.getKey();
          if (key.charAt(0) == '/') {
            key = key.substring(1);
          }
          key = key.substring(prefix.length());
          if (key.charAt(0) == '/') {
            key = key.substring(1);
          }
          int pos = key.indexOf("/");
          if (pos != -1) {
            key = key.substring(0, pos);
            dirs.add(key);
          }
        }
        req.setContinuationToken(result.getNextContinuationToken());
      }
      while (result.isTruncated() == true);

      List<String> ret = new ArrayList<>();
      for (String str : dirs) {
        ret.add(str);
      }
      return ret;
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }
}
