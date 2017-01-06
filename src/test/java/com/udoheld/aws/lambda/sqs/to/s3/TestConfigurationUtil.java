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

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationInitializer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.logging.Logger;

/**
 * @Author Udo Held
 */
public class TestConfigurationUtil {

  public static final String TEST_FILE_FOLDER = ".aws";
  public static final String TEST_FILE_NAME = "aws-lambda-transfer-sqs-to-s3-test.properties";

  public static AWSCredentials readCredentials(){
    AWSCredentials credentials = null;

    try {

      credentials = new ProfileCredentialsProvider().getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException(
          "Cannot load the credentials from the credential profiles file. " +
              "Please make sure that your credentials file is at the correct " +
              "location (~/.aws/credentials), and is in valid format.",
          e);
    }
    return credentials;
  }

  public static void loadLocalTestConfiguration(Logger log) throws IOException {
    purgeSystemEnvironmentVariables();

    File configFile = loadLocalTestConfigurationFile(log);

    ResourceBundle rb = loadResourceBundle(configFile, log);

    loadResourceBundleToSystemEnvironment(rb);
  }

  private static File loadLocalTestConfigurationFile(Logger log) throws FileNotFoundException {
    String userHome = System.getProperty("user.home");

    String configFilePath = userHome + File.separator + TEST_FILE_FOLDER + File.separator;
    configFilePath += TEST_FILE_NAME;
    File configFile = new File(configFilePath );

    if(!configFile.exists() || !configFile.isFile()){
      StringBuilder errorMsg = new StringBuilder();
      errorMsg.append("For the local test cases to work the test-configuration file \"");
      errorMsg.append(configFile.getAbsolutePath());
      errorMsg.append("\" must be present and readable. Yours doesn't exist.");
      errorMsg.append("Additionally the file must be changed pointing tog your own SQS queue ");
      errorMsg.append(" and your own S3 bucket.");
      log.severe(errorMsg.toString());
      throw new FileNotFoundException(configFile.getAbsolutePath());
    }

    return configFile;
  }

  private static ResourceBundle loadResourceBundle(File configFile, Logger log) throws IOException {
    ResourceBundle rb = null;

    try (FileInputStream fis = new FileInputStream(configFile)) {
      rb = new PropertyResourceBundle(fis);
    }

    StringBuilder errorMsg = new StringBuilder();
    if (!rb.containsKey(ConfigurationInitializer.CFG_SQS_QUEUE)
        || rb.getString(ConfigurationInitializer.CFG_SQS_QUEUE).isEmpty()){

      errorMsg.append("The key\"").append(ConfigurationInitializer.CFG_SQS_QUEUE);
      errorMsg.append("\" in file\"").append(configFile.getAbsolutePath());
      errorMsg.append("\" must be present and contain a valid configuration entry.");
    }

    if (!rb.containsKey(ConfigurationInitializer.CFG_S3_BUCKET_NAME)
        || rb.getString(ConfigurationInitializer.CFG_S3_BUCKET_NAME).isEmpty()){

      errorMsg.append("The key\"").append(ConfigurationInitializer.CFG_S3_BUCKET_NAME);
      errorMsg.append("\" in file\"").append(configFile.getAbsolutePath());
      errorMsg.append("\" must be present and contain a valid configuration entry.");
    }

    if (errorMsg.length() != 0) {
      log.severe(errorMsg.toString());
      throw new IllegalArgumentException(errorMsg.toString());
    }

    return rb;
  }

  private static void loadResourceBundleToSystemEnvironment(ResourceBundle rb) {
    rb.keySet().forEach(key -> System.setProperty(key,rb.getString(key)));
  }


  /**
   * Will delete the System environment variables. Race conditions may occur when running multiple
   * tests in parallel.
   */
  public static void purgeSystemEnvironmentVariables() {
    String [] configurationParameters = new String[]{ ConfigurationInitializer.CFG_DEBUG,
        ConfigurationInitializer.CFG_MAX_REMAINING_TIME_MS,
        ConfigurationInitializer.CFG_MAX_REMAINING_PCT,
        ConfigurationInitializer.CFG_SQS_QUEUE,
        ConfigurationInitializer.CFG_S3_BUCKET_NAME,
        ConfigurationInitializer.CFG_S3_BUCKET_FOLDER,
        ConfigurationInitializer.CFG_S3_FILE_PATTERN,
        ConfigurationInitializer.CFG_S3_FILE_SIZE_KB,
        ConfigurationInitializer.CFG_S3_UPLOAD_PART_SIZE_KB,
        ConfigurationInitializer.CFG_S3_MAX_MESSAGES_PER_FILE,
        ConfigurationInitializer.CFG_S3_RECORD_SEPARATOR,
        ConfigurationInitializer.CFG_S3_UPLOAD_THREADS_ENABLED,
        ConfigurationInitializer.CFG_S3_UPLOAD_THREADS_COUNT
    };
    Arrays.stream(configurationParameters)
        .filter(key -> System.getProperties().containsKey(key))
        .forEach(key -> System.getProperties().remove(key));
  }

  /**
   * Provides a logger.
   * @param log
   * @return
   */
  public static LambdaLogger initLambdaLogger(Logger log){
    LambdaLogger logger = new LambdaLogger() {
      @Override
      public void log(String msg) {
        log.info(msg);
      }
    };

    return logger;
  }

  /**
   * Implements a dummy context providing a logger.
   * @param log
   * @return
   */
  public static Context initContext(Logger log, int remainingRuntimeMillis) {
    Context context = new Context() {
      @Override
      public String getAwsRequestId() {
        return null;
      }

      @Override
      public String getLogGroupName() {
        return null;
      }

      @Override
      public String getLogStreamName() {
        return null;
      }

      @Override
      public String getFunctionName() {
        return null;
      }

      @Override
      public String getFunctionVersion() {
        return null;
      }

      @Override
      public String getInvokedFunctionArn() {
        return null;
      }

      @Override
      public CognitoIdentity getIdentity() {
        return null;
      }

      @Override
      public ClientContext getClientContext() {
        return null;
      }

      @Override
      public int getRemainingTimeInMillis() {
        return remainingRuntimeMillis;
      }

      @Override
      public int getMemoryLimitInMB() {
        return 0;
      }

      @Override
      public LambdaLogger getLogger() {
        return initLambdaLogger(log);
      }
    };
    return context;
  }
}
