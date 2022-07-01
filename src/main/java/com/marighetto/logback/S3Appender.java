package com.marighetto.logback;

import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RolloverFailure;
import ch.qos.logback.core.rolling.helper.FileNamePattern;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3Appender extends FixedWindowRollingPolicy {

  ExecutorService executor = Executors.newFixedThreadPool(1);

  String accessKey;
  String secretKey;

  String bucketEndpoint;
  String bucketRegion;
  String bucketFolder;
  String bucketName;

  boolean rollingOnExit = true;

  AmazonS3 s3Client;

  protected AmazonS3 getS3Client() {
    if (s3Client == null) {
      if (getAccessKey() == null
              || getSecretKey() == null
              || getBucketEndpoint() == null
              || getBucketRegion() == null
              || getBucketFolder() == null
              || getBucketName() == null) {
        s3Client = AmazonS3ClientBuilder.defaultClient();
      } else {
        BasicAWSCredentials credentials = new BasicAWSCredentials(getAccessKey(), getSecretKey());
        AWSStaticCredentialsProvider provider = new AWSStaticCredentialsProvider(credentials);

        s3Client =
                AmazonS3ClientBuilder.standard()
                        .withCredentials(provider)
                        .withEndpointConfiguration(
                                new AwsClientBuilder.EndpointConfiguration(
                                        getBucketEndpoint(), getBucketRegion()))
                        .build();
      }
    }

    return s3Client;
  }

  @Override
  public void start() {
    super.start();
    Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHookRunnable()));
  }

  @Override
  public void rollover() throws RolloverFailure {
    super.rollover();
    FileNamePattern fileNamePattern = new FileNamePattern(this.fileNamePatternStr, this.context);
    String rolledLogFileName = fileNamePattern.convertInt(getMinIndex());
    uploadFileToS3Async(rolledLogFileName);
  }

  protected void uploadFileToS3Async(String filename) {
    final File file = new File(filename);

    if (file.exists() && file.length() != 0) {
      long snapshot = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
      String key =
              getBucketFolder() + "/day=" + LocalDate.now() + "/" + snapshot + "-" + file.getName();

      addInfo("Uploading " + filename);

      Runnable uploader =
              () -> {
                try {
                  getS3Client().putObject(getBucketName(), key, file);
                } catch (Exception e) {
                  e.printStackTrace();
                }
              };

      executor.execute(uploader);
    } else {
      addError("File " + filename + " doesn't exist");
    }
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getBucketEndpoint() {
    return bucketEndpoint;
  }

  public void setBucketEndpoint(String bucketEndpoint) {
    this.bucketEndpoint = bucketEndpoint;
  }

  public String getBucketRegion() {
    return bucketRegion;
  }

  public void setBucketRegion(String bucketRegion) {
    this.bucketRegion = bucketRegion;
  }

  public String getBucketFolder() {
    return bucketFolder;
  }

  public void setBucketFolder(String bucketFolder) {
    this.bucketFolder = bucketFolder;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public boolean isrollingOnExit() {
    return rollingOnExit;
  }

  public void setRollingOnExit(boolean rollingOnExit) {
    this.rollingOnExit = rollingOnExit;
  }

  class ShutdownHookRunnable implements Runnable {
    @Override
    public void run() {
      try {
        if (isrollingOnExit()) {
          rollover();
        } else {
          uploadFileToS3Async(getActiveFileName());
        }
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (Exception e) {
        addError("Failed to upload a log in S3", e);
        executor.shutdown();
      }
    }
  }
}

