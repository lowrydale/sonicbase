package com.sonicbase.server;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AWSClient {

  public static final String KEYS_PART_STR = "/keys/";
  public static final String AWSKEYS_STR = "-awskeys";
  public static final String JSON_STR = ".json";
  public static final String USER_DIR_STR = "user.dir";
  private final DatabaseClient client;
  private static Logger logger = LoggerFactory.getLogger(AWSClient.class);

  private File installDir;
  private TransferManager transferManager;

  public AWSClient(DatabaseClient client) {
    this.client = client;
    ObjectNode config = getConfig();
    String dir = config.get("installDirectory").asText();
    installDir = new File(dir.replace("$HOME", System.getProperty("user.home")));
  }

  public TransferManager getTransferManager() {
    synchronized (this) {
      if (transferManager != null) {
        return transferManager;
      }
    }
    String cluster = client.getCluster();
    File keysFile = new File(installDir, KEYS_PART_STR + cluster + AWSKEYS_STR);
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
      File file = new File(System.getProperty(USER_DIR_STR), "config/config-" + cluster + JSON_STR);
      if (!file.exists()) {
        file = new File(System.getProperty(USER_DIR_STR), "src/main/resources/config/config-" + cluster + JSON_STR);
      }
      if (!file.exists()) {
        file = new File(System.getProperty(USER_DIR_STR), "../db/src/main/resources/config/config-" + cluster + JSON_STR);
      }
      String configStr = IOUtils.toString(new BufferedInputStream(new FileInputStream(file)), "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      return (ObjectNode) mapper.readTree(configStr);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public AmazonS3 getS3Client() {

    ClientConfiguration config = new ClientConfiguration();
    config.setConnectionTimeout(60_000);
    config.setSocketTimeout(6_000_000);
    config.setRequestTimeout(6_000_000);

    String cluster = client.getCluster();
    File keysFile = new File(installDir, KEYS_PART_STR + cluster + AWSKEYS_STR);
    if (!keysFile.exists()) {
      return new AmazonS3Client(new InstanceProfileCredentialsProvider(true), config);
    }
    else {
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
        String accessKey = reader.readLine();
        String secretKey = reader.readLine();

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        return new AmazonS3Client(awsCredentials, config);
      }
      catch (IOException e) {
        throw new DatabaseException(e);
      }
    }
  }

  public AmazonSQSClient getSQSClient() {
    final ClientConfiguration config = new ClientConfiguration();
    config.setMaxConnections(10);
    config.setRequestTimeout(20_000);
    config.setConnectionTimeout(60_000);

    String cluster = client.getCluster();
    File keysFile = new File(installDir, KEYS_PART_STR + cluster + AWSKEYS_STR);
    if (!keysFile.exists()) {
      return new AmazonSQSClient(new InstanceProfileCredentialsProvider(true), config);
    }
    else {
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
      while (result.isTruncated());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void uploadDirectory(final String bucket, final String prefix, final String path,
                              final File srcDir) {
      TransferManager localTransferManager = getTransferManager();
        MultipleFileUpload xfer = localTransferManager.uploadDirectory(bucket, prefix + "/" + path, srcDir, true);
    try {
      xfer.waitForCompletion();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
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
          Thread.currentThread().interrupt();
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
      ByteArrayOutputStream localOut;
      try (InputStream objectData = object.getObjectContent();
           ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        localOut = out;
        IOUtils.copy(objectData, out);
      }
      return localOut.toByteArray();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void downloadDirectory(final String bucket, String prefix, String subDirectory, File destDir) {
    TransferManager localTransferManager = getTransferManager();
    MultipleFileDownload download = localTransferManager.downloadDirectory(bucket, prefix + "/" + subDirectory, destDir, true);
    try {
      download.waitForCompletion();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DatabaseException(e);
    }

    File srcDir = new File(destDir, prefix + File.separator + subDirectory);
    File[] srcFiles = srcDir.listFiles();
    if (srcFiles != null) {
      for (File srcFile : srcFiles) {
        srcFile.renameTo(new File(destDir, srcFile.getName()));
      }
    }
    try {
      int pos = prefix.indexOf('/');
      if (pos != -1) {
        prefix = prefix.substring(0, pos);
      }
      FileUtils.deleteDirectory(new File(destDir, prefix));
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
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
          int pos = key.indexOf('/');
          if (pos != -1) {
            key = key.substring(0, pos);
            dirs.add(key);
          }
        }
        req.setContinuationToken(result.getNextContinuationToken());
      }
      while (result.isTruncated());

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

  public long getDirectorySize(String bucket, String prefix, String subDirectory) {
    long size = 0;
    AmazonS3 s3client = getS3Client();
    try {
      final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(prefix + "/" + subDirectory);
      ListObjectsV2Result result;
      do {
        result = s3client.listObjectsV2(req);

        for (S3ObjectSummary objectSummary :
            result.getObjectSummaries()) {
          size += objectSummary.getSize();
        }
        req.setContinuationToken(result.getNextContinuationToken());
      }
      while (result.isTruncated());
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    return size;
  }
}
