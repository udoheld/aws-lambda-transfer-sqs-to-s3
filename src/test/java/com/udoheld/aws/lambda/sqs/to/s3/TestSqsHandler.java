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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationInitializer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Udo Held
 */
public class TestSqsHandler {
  private AWSCredentials credentials;
  private Config config;
  private Context context;

  private static final int TEST_MESSAGES_WRITE_COUNT = 3;
  private static final int TEST_MAX_BATCH_COUNT = 10;
  private static final int TEST_MAX_BATCH_SIZE = (int) Math.pow(2,18);

  private Logger log = Logger.getLogger(this.getClass().getName());

  @Before
  public void initConfig() throws IOException {
    TestConfigurationUtil.purgeSystemEnvironmentVariables();
    TestConfigurationUtil.loadLocalTestConfiguration(log);

    credentials = TestConfigurationUtil.readCredentials();
    context = TestConfigurationUtil.initContext(log,20000);
    config = ConfigurationInitializer.initializeConfig(context,true);
  }

  @Test
  public void testRead() throws InterruptedException {
    writeTestMessages(config, TEST_MESSAGES_WRITE_COUNT, 2000 );

    List<SqsMessageHandler.MessageHolder> messages = null;

    SqsMessageHandler sqs = new SqsMessageHandler(config);
    try {
      messages = sqs.readMessages();
      assertNotNull(messages);
      assertTrue("At least one of the written test messages should be returned",
          messages.size() > 0);
    } finally {
      if (messages != null){
        List <String> messageIds = messages
            .stream()
            .map( msg -> msg.getMessageId())
            .collect(Collectors.toList());
        sqs.deleteMessages(messageIds);
      }
    }
  }

  @Test
  public void testEmptyDelete(){
    SqsMessageHandler sqs = new SqsMessageHandler(config);
    sqs.deleteMessages(new ArrayList<>());
  }

  @Ignore
  @Test
  public void writeSomeLargeMessages(){
    writeTestMessages(config,500,30000);
  }

  @Ignore
  @Test
  public void writeManySmallMessages(){
    writeTestMessages(config,100000,500);
  }

  public static void writeTestMessages(Config config, int msgCount, int msgSize){

    SendMessageBatchRequest request = new SendMessageBatchRequest(config.getSqsSourceQueue());

    DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE_TIME;
    String formattedDate = dtf.format(LocalDateTime.now());

    AmazonSQSClient sqsClient = new AmazonSQSClient();

    int batchMsgCount = 0;
    int batchMsgSize = 0;
    for (int i = 0; i < msgCount; i++){

      String msgBody = generateMessageBody(
          "Test message: " + i + " " + formattedDate, msgSize);

      if (batchMsgSize + msgBody.getBytes().length > TEST_MAX_BATCH_SIZE
          || batchMsgCount + 1 > TEST_MAX_BATCH_COUNT) {
        sqsClient.sendMessageBatch(request);
        batchMsgSize = 0;
        batchMsgCount = 0;
        request = new SendMessageBatchRequest(config.getSqsSourceQueue());
      }

      SendMessageBatchRequestEntry entry
          = new SendMessageBatchRequestEntry(Integer.toString(batchMsgCount),msgBody);

      request.getEntries().add(entry);
      batchMsgCount++;
      batchMsgSize += entry.getMessageBody().getBytes().length;
    }

    sqsClient.sendMessageBatch(request);
  }

  public static String generateMessageBody(String firstPart, int totalBytes){
    String messageBody = firstPart
        + RandomStringUtils.randomAlphanumeric(totalBytes/2 - firstPart.getBytes().length);
    return messageBody;
  }



}
