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

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class handles the reading of the messages from the SQS queue.
 *
 * @author Udo Held
 */
public class SqsMessageHandler {
  private Config config;
  private AmazonSQSClient sqsClient;

  private static final int SQS_DELETE_BATCH_LIMIT = 10;

  public SqsMessageHandler(Config config) {
    sqsClient = new AmazonSQSClient();
    this.config = config;
  }

  /**
   * Reads the next messages from the queue.
   * @return The read messages.
   */
  public List<MessageHolder> readMessages() {
    ReceiveMessageRequest request = new ReceiveMessageRequest(config.getSqsSourceQueue());
    request.setMaxNumberOfMessages(10);
    ReceiveMessageResult result = sqsClient.receiveMessage(request);

    List<MessageHolder> messages = mapReceivedMessages(result.getMessages());

    return messages;
  }

  /**
   * Delete the messages from the queue after being read.
   * @param messageIds Message references of the messages to be deleted.
   */
  public void deleteMessages(List<String> messageIds) {

    if (messageIds == null || messageIds.size() == 0) {
      return;
    }

    final AtomicInteger idCounter = new AtomicInteger();

    Function<String,DeleteMessageBatchRequestEntry> mapMessageId = messageId -> {
      DeleteMessageBatchRequestEntry entry =
          new DeleteMessageBatchRequestEntry(
              Integer.toString(idCounter.incrementAndGet()), messageId);
      return entry;
    };

    ExecutorService executor = Executors.newFixedThreadPool(config.getSqsDeletionThreads());

    for (int i = 0; i < messageIds.size(); i += SQS_DELETE_BATCH_LIMIT) {

      List<DeleteMessageBatchRequestEntry> entries = messageIds
          .stream()
          .skip(i)
          .limit(SQS_DELETE_BATCH_LIMIT)
          .map(mapMessageId)
          .collect(Collectors.toList());

      executor.execute(
          () -> { sqsClient.deleteMessageBatch(config.getSqsSourceQueue(), entries); }
      );
    }

    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException expected) {
      // The timeout is longer than the Lambda life-time.
    }
  }

  private List<MessageHolder> mapReceivedMessages(List<Message> receivedMessages) {
    Function<Message,MessageHolder> mapMessages = message -> {
      MessageHolder holder = new MessageHolder();
      holder.setMessage(message.getBody());
      holder.setMessageId(message.getReceiptHandle());
      return holder;
    };

    List<MessageHolder> messages = receivedMessages
        .stream()
        .map(mapMessages)
        .collect(Collectors.toList());
    return messages;
  }

  public class MessageHolder {
    private String message;
    private String messageId;

    public String getMessage() {
      return message;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public String getMessageId() {
      return messageId;
    }

    public void setMessageId(String messageId) {
      this.messageId = messageId;
    }
  }
}
