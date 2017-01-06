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
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationInitializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

/**
 * @author Udo Held
 */
public class TestTransferMessagesFromSqsToS3 {
  private Config config;
  private Context context;

  private Logger log = Logger.getLogger(this.getClass().getName());


  @Before
  public void initConfig() throws IOException {
    TestConfigurationUtil.purgeSystemEnvironmentVariables();
    TestConfigurationUtil.loadLocalTestConfiguration(log);

    context = TestConfigurationUtil.initContext(log,20000);
    config = ConfigurationInitializer.initializeConfig(context,true);
  }

  @Test
  public void testMessageUpload(){
    TransferMessagesFromSqsToS3.transferMessagesFromSqsToS3(config, context);
  }

  @Test
  public void testFilePattern() throws NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException, NoSuchFieldException {

    String testFileDatePattern = "yyyy-MM-dd'T'HH:mm:ss";
    String testFilePattern = "\"" + testFileDatePattern + "\"*.json";

    config.setS3FilePattern(testFilePattern);

    Constructor sqsToS3Con
        = TransferMessagesFromSqsToS3.class.getDeclaredConstructor(Config.class, Context.class);
    sqsToS3Con.setAccessible(true);
    TransferMessagesFromSqsToS3 sqsToS3
        = (TransferMessagesFromSqsToS3) sqsToS3Con.newInstance(config, context);
    Method initBFN = sqsToS3.getClass().getDeclaredMethod("initBaseFileName");
    initBFN.setAccessible(true);

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern(testFileDatePattern)
        .withZone(ZoneOffset.UTC);
    String beforeDate = dtf.format(Instant.now());
    // Generate file name
    initBFN.invoke(sqsToS3);
    String afterDate = dtf.format(Instant.now());

    Field baseFileNameF = sqsToS3.getClass().getDeclaredField("baseFileName");
    baseFileNameF.setAccessible(true);
    String baseFileName = (String) baseFileNameF.get(sqsToS3);

    String fullBefore = beforeDate + "*.json";
    String fullAfter = afterDate + "*.json";
    if (!fullBefore.equals(baseFileName)) {
      assertEquals("The dates should match.",fullAfter,baseFileName);
    }

  }

}
