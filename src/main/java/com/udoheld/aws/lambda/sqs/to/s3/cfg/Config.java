/*
    Copyright 2017 the original author or authors.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
 */

package com.udoheld.aws.lambda.sqs.to.s3.cfg;

/**
 * This Pojo holds the runtime configuration.
 * @author Udo Held
 */
public class Config {
  private boolean debug;
  private int lambdaMaxRemainingTimeMs;
  private int lambdaMaxRemainingPercentage;
  private int s3MaxMessagesPerFile;
  private String sqsSourceQueue;
  private int sqsDeletionThreads;
  private String s3BucketName;
  private String s3BucketFolder;
  private String s3FileInitiator;
  private String s3FilePattern;
  private String s3FileTerminator;
  private String s3RecordInitiator;
  private String s3RecordSeparator;
  private String s3RecordTerminator;
  private int s3MaxFileSizeKb;
  private int s3UploadPartSizeKb;
  private boolean s3UploadThreadsEnabled;
  private int s3UploadThreadCount;

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public int getLambdaMaxRemainingTimeMs() {
    return lambdaMaxRemainingTimeMs;
  }

  public void setLambdaMaxRemainingTimeMs(int lambdaMaxRemainingTimeMs) {
    this.lambdaMaxRemainingTimeMs = lambdaMaxRemainingTimeMs;
  }

  public int getLambdaMaxRemainingPercentage() {
    return lambdaMaxRemainingPercentage;
  }

  public void setLambdaMaxRemainingPercentage(int lambdaMaxRemainingPercentage) {
    this.lambdaMaxRemainingPercentage = lambdaMaxRemainingPercentage;
  }

  public int getS3MaxMessagesPerFile() {
    return s3MaxMessagesPerFile;
  }

  public void setS3MaxMessagesPerFile(int s3MaxMessagesPerFile) {
    this.s3MaxMessagesPerFile = s3MaxMessagesPerFile;
  }

  public String getSqsSourceQueue() {
    return sqsSourceQueue;
  }

  public void setSqsSourceQueue(String sqsSourceQueue) {
    this.sqsSourceQueue = sqsSourceQueue;
  }

  public int getSqsDeletionThreads() {
    return sqsDeletionThreads;
  }

  public void setSqsDeletionThreads(int sqsDeletionThreads) {
    this.sqsDeletionThreads = sqsDeletionThreads;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public void setS3BucketName(String s3BucketName) {
    this.s3BucketName = s3BucketName;
  }

  public String getS3BucketFolder() {
    return s3BucketFolder;
  }

  public void setS3BucketFolder(String s3BucketFolder) {
    this.s3BucketFolder = s3BucketFolder;
  }

  public String getS3FileInitiator() {
    return s3FileInitiator;
  }

  public void setS3FileInitiator(String s3FileInitiator) {
    this.s3FileInitiator = s3FileInitiator;
  }

  public String getS3FilePattern() {
    return s3FilePattern;
  }

  public void setS3FilePattern(String s3FilePattern) {
    this.s3FilePattern = s3FilePattern;
  }

  public String getS3FileTerminator() {
    return s3FileTerminator;
  }

  public void setS3FileTerminator(String s3FileTerminator) {
    this.s3FileTerminator = s3FileTerminator;
  }

  public int getS3MaxFileSizeKb() {
    return s3MaxFileSizeKb;
  }

  public void setS3MaxFileSizeKb(int s3MaxFileSizeKb) {
    this.s3MaxFileSizeKb = s3MaxFileSizeKb;
  }

  public String getS3RecordInitiator() {
    return s3RecordInitiator;
  }

  public void setS3RecordInitiator(String s3RecordInitiator) {
    this.s3RecordInitiator = s3RecordInitiator;
  }

  public String getS3RecordSeparator() {
    return s3RecordSeparator;
  }

  public void setS3RecordSeparator(String s3RecordSeparator) {
    this.s3RecordSeparator = s3RecordSeparator;
  }

  public String getS3RecordTerminator() {
    return s3RecordTerminator;
  }

  public void setS3RecordTerminator(String s3RecordTerminator) {
    this.s3RecordTerminator = s3RecordTerminator;
  }

  public int getS3UploadPartSizeKb() {
    return s3UploadPartSizeKb;
  }

  public void setS3UploadPartSizeKb(int s3UploadPartSizeKb) {
    this.s3UploadPartSizeKb = s3UploadPartSizeKb;
  }

  public boolean isS3UploadThreadsEnabled() {
    return s3UploadThreadsEnabled;
  }

  public void setS3UploadThreadsEnabled(boolean s3UploadThreadsEnabled) {
    this.s3UploadThreadsEnabled = s3UploadThreadsEnabled;
  }

  public int getS3UploadThreadCount() {
    return s3UploadThreadCount;
  }

  public void setS3UploadThreadCount(int s3UploadThreadCount) {
    this.s3UploadThreadCount = s3UploadThreadCount;
  }
}
