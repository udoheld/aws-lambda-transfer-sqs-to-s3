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

import com.amazonaws.services.lambda.runtime.Context;

/**
 * This class helps initializing the configuration.
 * @author Udo Held
 */
public class ConfigurationInitializer {

  public static final String CFG_DEBUG = "debug";
  public static final String CFG_MAX_REMAINING_TIME_MS = "Lambda_Max_Remaining_Time_MS";
  public static final String CFG_MAX_REMAINING_PCT
      = "Lambda_Max_Remaining_Time_Percentage";
  public static final String CFG_SQS_QUEUE = "SQS_Source_Queue";
  public static final String CFG_SQS_DELETION_THREADS = "SQS_Deletion_Threads";
  public static final String CFG_S3_BUCKET_NAME = "S3_Bucket_Name";
  public static final String CFG_S3_BUCKET_FOLDER = "S3_Bucket_Folder";
  public static final String CFG_S3_FILE_INITIATOR = "S3_File_Initiator";
  public static final String CFG_S3_FILE_PATTERN = "S3_File_Pattern";
  public static final String CFG_S3_FILE_SIZE_KB = "S3_File_Size_KB";
  public static final String CFG_S3_FILE_TERMINATOR = "S3_File_Terminator";
  public static final String CFG_S3_UPLOAD_PART_SIZE_KB = "S3_Upload_Part_Size_KB";
  public static final String CFG_S3_MAX_MESSAGES_PER_FILE = "S3_Max_Messages_Per_File";
  public static final String CFG_S3_RECORD_INITIATOR = "S3_Record_Initiator";
  public static final String CFG_S3_RECORD_SEPARATOR = "S3_Record_Separator";
  public static final String CFG_S3_RECORD_TERMINATOR = "S3_Record_Terminator";
  public static final String CFG_S3_UPLOAD_THREADS_ENABLED = "S3_Upload_Threads_Enabled";
  public static final String CFG_S3_UPLOAD_THREADS_COUNT = "S3_Upload_Threads_Count";

  private static final String S3_FILE_PATTERN_WILDCARD = "*";

  private static final int S3_MINIMUM_UPLOAD_PART_SIZE = 5120;

  private final Config config;
  private final Context context;
  private StringBuilder debugLogBuilder = new StringBuilder();
  private boolean debug = false;
  private final String linSep = System.lineSeparator();

  private ConfigurationInitializer(Context context, boolean forceDebug) {
    config = new Config();
    this.context = context;
    this.debug = forceDebug;
  }

  public static Config initializeConfig(Context context, boolean forceDebug) {
    ConfigurationInitializer cfgInit = new ConfigurationInitializer(context, forceDebug);
    return cfgInit.readConfig();
  }

  private Config readConfig() {
    int startingTimeRemaining = context.getRemainingTimeInMillis();

    debugLogBuilder.append("Reading configuration." + linSep);

    if (!debug) {
      debug = readValue(CFG_DEBUG,false);
      config.setDebug(debug);
    }

    initTimeRemaining(startingTimeRemaining);

    initSqs();

    initS3();

    if (debug) {
      debugLogBuilder.append("Read configuration!" + linSep);
      context.getLogger().log(debugLogBuilder.toString());
    }

    validateConfiguration();

    return config;
  }

  private void initTimeRemaining(int startingTimeRemaining) {
    config.setLambdaMaxRemainingTimeMs(readValue(CFG_MAX_REMAINING_TIME_MS,0));
    if (config.getLambdaMaxRemainingTimeMs() == 0) {
      config.setLambdaMaxRemainingPercentage(readValue(CFG_MAX_REMAINING_PCT,70));
      config.setLambdaMaxRemainingTimeMs(
          startingTimeRemaining * config.getLambdaMaxRemainingPercentage() / 100);
      if (debug) {
        debugLogBuilder.append("Calculated absolute time remaining from " + CFG_MAX_REMAINING_PCT
            + " with a total value of \"" + config.getLambdaMaxRemainingTimeMs() + "\" ms."
            + linSep);
      }
    }
  }

  private void initSqs() {
    config.setSqsSourceQueue(readValue(CFG_SQS_QUEUE,""));
    config.setSqsDeletionThreads(readValue(CFG_SQS_DELETION_THREADS,5));
  }

  private void initS3() {
    config.setS3BucketName(readValue(CFG_S3_BUCKET_NAME,""));
    config.setS3BucketFolder(readValue(CFG_S3_BUCKET_FOLDER,""));
    config.setS3FilePattern(readValue(CFG_S3_FILE_PATTERN,
        "\"yyyy-MM-dd'T'HH:mm:ss\"*.json"));
    config.setS3MaxFileSizeKb(readValue(CFG_S3_FILE_SIZE_KB,10240));
    config.setS3UploadPartSizeKb(readValue(CFG_S3_UPLOAD_PART_SIZE_KB,
        S3_MINIMUM_UPLOAD_PART_SIZE));
    if (config.getS3UploadPartSizeKb() < S3_MINIMUM_UPLOAD_PART_SIZE) {

      if (debug) {
        debugLogBuilder.append( "\"" + CFG_S3_UPLOAD_PART_SIZE_KB
            + "\" size is smaller than the minimum value \"" + S3_MINIMUM_UPLOAD_PART_SIZE
            + "\". Overwriting \"" + CFG_S3_UPLOAD_PART_SIZE_KB + "\" with the minimum value."
            + linSep);
      }
      config.setS3UploadPartSizeKb(S3_MINIMUM_UPLOAD_PART_SIZE);
    }

    config.setS3MaxMessagesPerFile(readValue(CFG_S3_MAX_MESSAGES_PER_FILE,10000));
    config.setS3FileInitiator(readValue(CFG_S3_FILE_INITIATOR,""));
    config.setS3FileTerminator(readValue(CFG_S3_FILE_TERMINATOR, ""));
    config.setS3RecordInitiator(readValue(CFG_S3_RECORD_INITIATOR, ""));
    config.setS3RecordSeparator(readValue(CFG_S3_RECORD_SEPARATOR, linSep));
    config.setS3RecordTerminator(readValue(CFG_S3_RECORD_TERMINATOR, ""));
    config.setS3UploadThreadsEnabled(readValue(CFG_S3_UPLOAD_THREADS_ENABLED,true));
    config.setS3UploadThreadCount(readValue(CFG_S3_UPLOAD_THREADS_COUNT,2));

  }

  private int readValue(String key, int defaultValue) {
    int value = defaultValue;
    String envValue = readEnvironmentEntry(key);
    if (envValue != null) {
      try {
        value = Integer.parseInt(envValue);
      } catch (NumberFormatException e) {
        if (debug) {
          debugLogBuilder.append("Error parsing value " + key + " " + e.getMessage()
              + linSep);
        }
      }
      if (debug) {
        debugLogBuilder.append("Found value for key: " + key + " value: " + value
            + linSep);
      }
    } else {
      if (debug) {
        debugLogBuilder.append("No valid value found for key: " + key + " using default: "
            + value + linSep);
      }
    }
    return value;
  }

  private boolean readValue(String key, boolean defaultValue) {
    boolean value = defaultValue;
    String envValue = readEnvironmentEntry(key);
    if (envValue != null && !envValue.isEmpty()) {
      value = envValue.equalsIgnoreCase("true") || envValue.equals("1");

      if (debug) {
        debugLogBuilder.append("Found value for key: " + key + " value: " + value
            + linSep);
      }
    } else {
      if (debug) {
        debugLogBuilder.append("No valid value found for key: " + key + " using default: "
            + value + linSep);
      }
    }
    return value;
  }

  private String readValue(String key, String defaultValue) {
    String value = defaultValue;
    String envValue = readEnvironmentEntry(key);
    if (envValue != null && !envValue.isEmpty()) {
      value = envValue;
      if (debug) {
        debugLogBuilder.append("Found value for key: " + key + " value: " + value
            + linSep);
      }
    } else {
      if (debug) {
        debugLogBuilder.append("No valid value found for key: " + key + " using default: "
            + value + linSep);
      }
    }
    return value;
  }

  private String readEnvironmentEntry(String key) {
    if (System.getenv().containsKey(key)) {
      return System.getenv().get(key);
      //Fallback to properties for testing.
    } else if (System.getProperties().containsKey(key)) {
      return System.getProperty(key);
    } else {
      return null;
    }
  }

  private void validateConfiguration() {
    boolean valid = true;

    StringBuilder valErrors = new StringBuilder();
    valErrors.append("Error during configuration validation.").append(linSep);

    if (context.getRemainingTimeInMillis() < config.getLambdaMaxRemainingTimeMs()) {
      valid = false;
      valErrors.append("ERROR: The remaining time \"" + context.getRemainingTimeInMillis()
          + "ms\" is smaller than the configured maximum remaining time \""
          + config.getLambdaMaxRemainingTimeMs()
          + "ms\". Configure a smaller value for the environment variable \""
          + CFG_MAX_REMAINING_TIME_MS
          + "\" or increase the total Lambda timeout in the AWS Management Console" + linSep);
    }
    if (config.getSqsSourceQueue() == null || config.getSqsSourceQueue().isEmpty()) {
      valid = false;
      valErrors.append("ERROR: A valid SQS source queue name for the environment variable \""
          + CFG_SQS_QUEUE + "\" must be configured in the AWS Management Console." + linSep);
    }

    if (config.getS3BucketName() == null || config.getS3BucketName().isEmpty()) {
      valid = false;
      valErrors.append("ERROR: A valid S3 bucket name for the environment variable \""
          + CFG_S3_BUCKET_NAME + "\" must be configured in the AWS Management Console." + linSep);
    }

    if (config.getS3FilePattern() == null || config.getS3FilePattern().isEmpty()
        || !config.getS3FilePattern().contains(S3_FILE_PATTERN_WILDCARD)) {
      valid = false;
      valErrors.append("ERROR: The S3 File pattern for the environment variable \""
          + CFG_S3_FILE_PATTERN + "\" must be configured and contain the wildcard \""
          + S3_FILE_PATTERN_WILDCARD + "\" in the AWS Management Console." + linSep);
    }

    if (!valid) {
      context.getLogger().log(valErrors.toString());
      throw new IllegalArgumentException(valErrors.toString());
    }
  }
}

