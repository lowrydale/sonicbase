package com.sonicbase.common;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.clouddirectory.model.DeleteObjectRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.sonicbase.client.DatabaseClient;
import com.sonicbase.query.DatabaseException;

import java.io.*;

/**
 * Created by lowryda on 6/16/17.
 */
public class AWSClient {

  private final DatabaseClient client;
  private final Logger logger;

  public AWSClient(DatabaseClient client) {
    this.client = client;
    this.logger = new Logger(client);

  }

  public static TransferManager getTransferManager() {
    File keysFile = new File(System.getProperty("user.home"), ".awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(".awskeys file not found");
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

  public static AmazonS3 getS3Client() {
    File keysFile = new File(System.getProperty("user.home"), ".awskeys");
    if (!keysFile.exists()) {
      throw new DatabaseException(".awskeys file not found");
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
      s3client.deleteObject(bucket, prefix);
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  public void uploadDirectory(String bucket, String prefix, final String path, final File srcFile) {
    TransferManager transferManager = getTransferManager();
    try {
      MultipleFileUpload xfer = transferManager.uploadDirectory(bucket,
          prefix + path, srcFile, true);
      xfer.waitForCompletion();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      transferManager.shutdownNow();
    }
  }

  public void downloadDirectory(String bucket, String prefix, String subDirectory, File destDir) {
    TransferManager transferManager = getTransferManager();
    try {
      MultipleFileDownload xfer = transferManager.downloadDirectory(
          bucket, prefix + subDirectory, destDir);
      // loop with Transfer.isDone()
      xfer.waitForCompletion();
    }
    catch (Exception e) {
      throw new DatabaseException(e);
    }
    finally {
      transferManager.shutdownNow();
    }
  }
}
