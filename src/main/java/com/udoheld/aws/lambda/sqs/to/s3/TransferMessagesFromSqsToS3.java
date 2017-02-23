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

package com.udoheld.aws.lambda.sqs.to.s3;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transfers messages from SQS to S3.
 *
 * @author Udo Held
 */
public class TransferMessagesFromSqsToS3 {

  private final Config config;
  private final Context context;
  private SqsMessageHandler sqsMessageHandler;
  private S3MultiPartFileHandler s3MultiPartFileHandler;

  private int fileNumber = 0;
  private int fileSize = 0;

  private boolean firstRecordInFile = true;

  private List<String> transferredMsgIds = new ArrayList<>();
  private List<SqsMessageHandler.MessageHolder> readMessagesL;

  private ByteArrayOutputStream fileBufferOs = new ByteArrayOutputStream();
  private String baseFileName;

  private static String lineSep = System.lineSeparator();

  private TransferMessagesFromSqsToS3(Config config, Context context) {
    this.config = config;
    this.context = context;
  }

  /**
   * Reads the current messages fom SQS and stores them in a S3 file.
   * @param config Configuration.
   * @param context AWS Lambda context.
   */
  public static void transferMessagesFromSqsToS3(Config config, Context context) {
    TransferMessagesFromSqsToS3 sqsToS3
        = new TransferMessagesFromSqsToS3(config, context);
    sqsToS3.transferMessages();
  }

  private void transferMessages() {
    init();

    while (hasTimeForMoreMessages() && readMessages()) {
      processMessages();
    }
    flushFileBufferToS3(true);
  }

  private void init() {
    sqsMessageHandler = new SqsMessageHandler(config);
    initBaseFileName();
  }

  /**
   * Will set the baseFileName. It should contain two double quotes, a valid format
   * following the patterns in {@link java.time.format.DateTimeFormatter} in between
   * the double quotes and a wildcard for increasing the number, if multiple files
   * are written. The general format is "[prefix]"[dateformat]"*[postfix]"
   * Examples are ""yyyy-MM-dd'T'HH:mm:ss"*.json" and "s3"dd/MM/yyyy hh:mm:ss aa"*.log".
   */
  private void initBaseFileName() {
    String subPattern = "\".*\"";
    String pattern = ".*(" + subPattern + ").*";

    Pattern logEntry = Pattern.compile(pattern);
    Matcher matchPattern = logEntry.matcher(config.getS3FilePattern());

    if (matchPattern.find()) {
      String matchedGroup = matchPattern.group(1).replaceFirst("\"","").replaceAll("\"$","");

      DateTimeFormatter dtf = DateTimeFormatter.ofPattern(matchedGroup);
      String curDate = dtf.withZone(ZoneOffset.UTC).format(Instant.now());
      baseFileName = config.getS3FilePattern().replaceFirst(subPattern,curDate);
    }
  }

  /**
   * Checks if there is enough time for processing more messages according the the configured
   * threshold and the actual time left.
   * @return true if more messages can be processed.
   */
  private boolean hasTimeForMoreMessages() {
    boolean hasMoreTime
        = context.getRemainingTimeInMillis() > config.getLambdaMaxRemainingTimeMs();
    if (!hasMoreTime && config.isDebug()) {
      context.getLogger().log(
          "Stopped processing further messages, because of lack of time." + lineSep);
    }
    return hasMoreTime;
  }

  /**
   * Reads messages from SQS
   * @return true, if messages were present and have been read.
   */
  private boolean readMessages() {
    readMessagesL = sqsMessageHandler.readMessages();
    return readMessagesL != null && readMessagesL.size() > 0;
  }

  /**
   * Processes the already read messages from SQS.
   */
  private void processMessages() {
    for (SqsMessageHandler.MessageHolder message : readMessagesL) {
      processMessage(message);
      checkAndFlushFileBuffer();
    }
  }

  /**
   * Reads a single message into the write buffer adding seperators.
   * @param message Message to write.
   */
  private void processMessage(SqsMessageHandler.MessageHolder message) {
    try {
      // File initiator
      if (firstRecordInFile && ! config.getS3FileInitiator().isEmpty()) {
        fileBufferOs.write(config.getS3FileInitiator().getBytes());
      }

      // Record separator, for non first records
      if (fileBufferOs.size() > 0 && ! firstRecordInFile
          && ! config.getS3RecordSeparator().isEmpty()) {
        fileBufferOs.write(config.getS3RecordSeparator().getBytes());
      }
      firstRecordInFile = false;
      // Record initiator
      if (! config.getS3RecordInitiator().isEmpty()) {
        fileBufferOs.write(config.getS3RecordInitiator().getBytes());
      }

      fileBufferOs.write(message.getMessage().getBytes());
      // Record terminator
      if (! config.getS3RecordTerminator().isEmpty()) {
        fileBufferOs.write(config.getS3RecordTerminator().getBytes());
      }

      transferredMsgIds.add(message.getMessageId());

    } catch (IOException expected) {
    }
  }

  /**
   * Checks if the message or file-size thresholds have been reached and writes file to S3.
   */
  private void checkAndFlushFileBuffer() {
    // Write file
    if (transferredMsgIds.size() >= config.getS3MaxMessagesPerFile()
        || fileSize + fileBufferOs.size() > config.getS3MaxFileSizeKb() * 1024) {
      flushFileBufferToS3(true);
      fileNumber++;
    // Write part only
    } else if (fileBufferOs.size() > config.getS3UploadPartSizeKb() * 1024) {
      flushFileBufferToS3(false);
    }
  }

  /**
   * Transfers the current message buffer to S3.
   * @param finalizeFile Finalises the file, adding the file terminator and closing the
   *                     MultiPartUpload.
   */
  private void flushFileBufferToS3(boolean finalizeFile) {
    if (transferredMsgIds.size() == 0) {
      if (config.isDebug()) {
        context.getLogger().log("No messages to transfer." + lineSep);
      }
      return;
    }

    if (config.isDebug()) {
      context.getLogger().log("Writing file to S3 with size \"" + fileBufferOs.size()
          + "\"b with \"" + transferredMsgIds.size() + "\" messages." + lineSep);
    }
    if (s3MultiPartFileHandler == null) {
      startNewFile();
    }

    uploadPart(false);

    if (finalizeFile) {
      finalizeFile();
    }
  }

  private void startNewFile() {
    String fileName = fileNumber == 0 ? baseFileName.replace("*","")
        : baseFileName.replace("*","-" + Integer.toString(fileNumber));

    if (config.isDebug()) {
      context.getLogger().log("Starting new file \"" + fileName + "\"." + lineSep );
    }
    s3MultiPartFileHandler =
        S3MultiPartFileHandler.startFileUpload(fileName, config, context.getLogger());
  }

  /**
   * Uploads a S3 Multi Part upload file part.
   * @param lastPart Set the file separator if it is the last part.
   */
  private void uploadPart(boolean lastPart) {
    if (! config.getS3FileTerminator().isEmpty()) {
      try {
        fileBufferOs.write(config.getS3FileTerminator().getBytes());
      } catch (IOException expected) {
      }
    }
    s3MultiPartFileHandler.uploadPart(fileBufferOs.toByteArray());
    fileSize += fileBufferOs.size();
    fileBufferOs = new ByteArrayOutputStream();
  }

  /**
   * Closes the S3 multipart upload and deletes the messages.
   */
  private void finalizeFile() {
    if (config.isDebug()) {
      context.getLogger().log("Finalizing file upload." + lineSep);
    }
    if (! s3MultiPartFileHandler.finalizeMultipartUpload()) {
      throw new AmazonS3Exception("Error during file uploading. Aborting processing.");
    }
    s3MultiPartFileHandler = null;
    sqsMessageHandler.deleteMessages(transferredMsgIds);
    transferredMsgIds = new ArrayList<>();
    firstRecordInFile = true;
  }
}
