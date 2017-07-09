package com.sonicbase.common;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.clouddirectory.model.DeleteObjectRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;
import com.sonicbase.util.JsonDict;
import com.sonicbase.util.StreamUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.Arrays.asList;

/**
 * Created by lowryda on 6/16/17.
 */
public class AWSClient {

  private final DatabaseClient client;
  private final Logger logger;
  private File installDir;

  public AWSClient(DatabaseClient client) {
    this.client = client;
    this.logger = new Logger(client);

  }

  private ThreadPoolExecutor executor = new ThreadPoolExecutor(8, 8, 10000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

  public TransferManager getTransferManager() {
    File installDir = getInstallDir();
    String cluster = client.getCluster();
    File keysFile = new File(installDir, "/keys/" + cluster +"-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    BasicAWSCredentials awsCredentials = null;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

      return new TransferManager(awsCredentials);
    }
    catch (IOException e) {
      throw new DatabaseException(e);
    }
  }

  private JsonDict getConfig() {
    try {
      String cluster = client.getCluster();
      File file = new File(System.getProperty("user.dir"), "config/config-" + cluster + ".json");
      if (!file.exists()) {
        file = new File(System.getProperty("user.dir"), "db/src/main/resources/config/config-" + cluster + ".json");
      }
      String configStr = StreamUtils.inputStreamToString(new BufferedInputStream(new FileInputStream(file)));
      return new JsonDict(configStr);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public File getInstallDir() {
    if (installDir == null) {
      JsonDict config = getConfig();
      String dir = config.getString("installDirectory");
      installDir = new File(dir.replace("$HOME", System.getProperty("user.home")));
    }
    return installDir;
  }

  public AmazonS3 getS3Client() {
    File installDir = getInstallDir();
    String cluster = client.getCluster();
    File keysFile = new File(installDir, "/keys/" + cluster +"-awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(cluster + "-awskeys file not found");
    }
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(keysFile)))) {
      String accessKey = reader.readLine();
      String secretKey = reader.readLine();

      BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

      return new AmazonS3Client(awsCredentials);
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

  public void uploadDirectory(String bucket, String prefix, final String path, final File srcFile) {
    for (int i = 0; i < 10; i++) {
      TransferManager transferManager = getTransferManager();
      try {
        MultipleFileUpload xfer = transferManager.uploadDirectory(bucket, prefix + "/" + path, srcFile, true);
        xfer.waitForCompletion();
        //      File[] files = srcFile.listFiles();
        //      if (files != null) {
        //        for (File file : files) {
        //          if (file.isDirectory()) {
        //            uploadDirectory(bucket, prefix, path + "/" + file.getName(), file);
        //          }
        //          else {
        //            uploadFile(bucket, prefix, path, file);
        //          }
        //        }
        //      }
        break;
      }
      catch (Exception e) {
        logger.error("Error uploading directory: srcDir=" + srcFile.getAbsolutePath(), e);
        if (i == 9) {
          throw new DatabaseException(e);
        }
      }
      finally {
        transferManager.shutdownNow();
      }

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
        StreamUtils.copyStream(objectData, out);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void downloadFile(String bucket, String key, File destFile) {
    AmazonS3 s3client = getS3Client();
    try {
      S3Object object = s3client.getObject(
          new GetObjectRequest(bucket, key));
      destFile.getParentFile().mkdirs();
      try (InputStream objectData = object.getObjectContent();
           BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(destFile))) {
        StreamUtils.copyStream(objectData, out);
      }
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void downloadDirectory(final String bucket, String prefix, String subDirectory, File destDir) {
    TransferManager transferManager = getTransferManager();
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
          futures.add(executor.submit(new Runnable(){
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
