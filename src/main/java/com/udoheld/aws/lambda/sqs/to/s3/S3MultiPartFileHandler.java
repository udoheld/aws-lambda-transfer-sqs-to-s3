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

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Writes the input to S3 allowing for the splitting of multiple parts.
 *
 * @author Udo Held
 */
public class S3MultiPartFileHandler {
  private Config config;
  private String uploadId;
  private final AmazonS3Client s3Client;
  private int partCounter = 0;
  private ExecutorService executor;
  private boolean useThreads = false;
  private List<Future<PartETag>> parts = new ArrayList<>();
  private final LambdaLogger log;
  private String fileNameKey;
  private String bucketName;

  /**
   * Method for starting a multiple part upload.
   * @param fileName Filename or keyname in S3 excluding path.
   * @param config Lambda configuration.
   * @param log AWS Logger
   * @return A handler for uploading the parts.
   */
  public static S3MultiPartFileHandler startFileUpload(String fileName, Config config,
                                                       LambdaLogger log) {
    S3MultiPartFileHandler s3Handler = new S3MultiPartFileHandler(config, log);
    s3Handler.initMultipartUpload(fileName);
    return s3Handler;
  }

  private S3MultiPartFileHandler(Config config, LambdaLogger log) {
    this.config = config;
    s3Client = new AmazonS3Client();
    this.log = log;
  }

  /**
   * Reading the configuration and calling S3 for the retrieval of the upload id.
   * @param fileName S3 key name
   */
  private void initMultipartUpload(String fileName) {
    initThreading();

    bucketName = config.getS3BucketName();
    fileNameKey = getFullKeyName(fileName);

    InitiateMultipartUploadRequest request =
        new InitiateMultipartUploadRequest(bucketName, fileNameKey);

    InitiateMultipartUploadResult response = s3Client.initiateMultipartUpload(request);
    uploadId = response.getUploadId();
  }

  private void initThreading() {
    useThreads = config.isS3UploadThreadsEnabled();
    if (useThreads) {
      int threadCount = config.getS3UploadThreadCount() > 0 ? config.getS3UploadThreadCount() : 1;
      executor = Executors.newFixedThreadPool(threadCount);
    }
  }

  /**
   * Generates the full S3 key/filename including folders.
   * @param fileName Filename.
   * @return Full key name including folder from the configuration.
   */
  private String getFullKeyName(String fileName) {
    if (config.getS3BucketFolder() != null && !config.getS3BucketFolder().isEmpty()) {
      return config.getS3BucketFolder() + "/" + fileName;
    } else {
      return fileName;
    }
  }


  /**
   * Uploads a part to AWS. Each part, but the last must be larger than 5 MB as required by the
   * S3-API.
   * @param input Input as bytes.
   */
  public synchronized void uploadPart(byte[] input)  {
    ByteArrayInputStream bais = new ByteArrayInputStream(input);

    final UploadPartRequest request = new UploadPartRequest();
    request
        .withBucketName(bucketName)
        .withKey(fileNameKey)
        .withUploadId(uploadId)
        .withPartNumber(++partCounter)
        .withPartSize(input.length)
        .withInputStream(bais);
    try {
      byte [] md5Hash = MessageDigest.getInstance("MD5").digest(input);
      String md5enc = Base64.getEncoder().encodeToString(md5Hash);
      request.withMD5Digest(md5enc);
    } catch (NoSuchAlgorithmException expected) {
    }

    Callable<PartETag> callable = () -> {
      UploadPartResult result = s3Client.uploadPart(request);
      return new PartETag(result.getPartNumber(),result.getETag());
    };

    if (useThreads) {
      parts.add(executor.submit(callable));
    } else {
      uploadPartUnthreaded(callable);
    }
  }

  /**
   * Direct upload processing instead of using an Executor.
   * This will postpone any exceptions to the calling of the get method.
   * @param callable upload part callable.
   */
  private void uploadPartUnthreaded(Callable<PartETag> callable) {
    PartETag partHolder = null;

    try {
      callable.call();
      Future<PartETag> partFuture = new PartETagFuture(partHolder, null);
      parts.add(partFuture);
    } catch (Exception e) {
      Future<PartETag> partFuture = new PartETagFuture(null, e);
      parts.add(partFuture);
    }
  }

  /**
   * Concludes an upload and merges the parts.
   * @return true if merging was successful.
   */
  public synchronized boolean finalizeMultipartUpload() {
    boolean uploadSuccess = false;
    try {
      List<PartETag> partETags = new ArrayList<>();
      for (Future<PartETag> part : parts) {
        partETags.add(part.get());
      }

      CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest();
      request
          .withBucketName(bucketName)
          .withKey(fileNameKey)
          .withUploadId(uploadId)
          .withPartETags(partETags);

      // Throws an error if unsuccessful.
      s3Client.completeMultipartUpload(request);
      uploadSuccess = true;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AmazonS3Exception) {
        AmazonS3Exception s3Exception = (AmazonS3Exception) e.getCause();
        log.log("Error uploading parts.\n" + s3Exception.getMessage() + "\nresponseXml:"
            + s3Exception.getErrorResponseXml());
      } else {
        log.log("Error uploading parts.\n" + e.getMessage());
      }

    } catch (Exception e) {
      log.log("Error uploading parts.\n" + e.getMessage());
    }

    if (!uploadSuccess) {
      abortUpload();
    }
    return uploadSuccess;
  }

  private void abortUpload() {
    AbortMultipartUploadRequest request =
        new AbortMultipartUploadRequest(config.getS3BucketName(), fileNameKey,uploadId);
    s3Client.abortMultipartUpload(request);
  }

  private class PartETagFuture implements Future<PartETag> {
    private PartETag partETag;
    private Exception exception;

    PartETagFuture(PartETag partETag, Exception exception) {
      this.partETag = partETag;
      this.exception = exception;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public PartETag get() throws InterruptedException, ExecutionException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }

      return partETag;
    }

    @Override
    public PartETag get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      if (exception != null) {
        throw new ExecutionException(exception);
      }
      return partETag;
    }
  }
}
