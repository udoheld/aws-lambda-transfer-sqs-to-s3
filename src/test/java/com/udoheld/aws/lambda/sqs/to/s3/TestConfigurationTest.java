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
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationInitializer;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationTest;
import org.junit.Test;

import java.io.*;
import java.util.logging.Logger;

/**
 * @author Udo Held
 */
public class TestConfigurationTest {

  private Logger log = Logger.getLogger(this.getClass().getName());

  @Test(expected = IllegalArgumentException.class)
  public void testContextConfigurationMissingInvalid() throws IOException {
    TestConfigurationUtil.purgeSystemEnvironmentVariables();

    Context context = TestConfigurationUtil.initContext(log,2000);

    ConfigurationTest configurationTest = new ConfigurationTest();
    configurationTest.handleRequest(
        new ByteArrayInputStream(new byte[]{}),new ByteArrayOutputStream(),context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContextConfigurationTimeoutToLow() throws IOException {
    TestConfigurationUtil.purgeSystemEnvironmentVariables();
    System.setProperty(ConfigurationInitializer.CFG_SQS_QUEUE,"dummyQ");
    System.setProperty(ConfigurationInitializer.CFG_S3_BUCKET_NAME,"dummyBucket");

    System.setProperty(ConfigurationInitializer.CFG_MAX_REMAINING_TIME_MS,"200");

    Context context = TestConfigurationUtil.initContext(log,100);

    ConfigurationTest configurationTest = new ConfigurationTest();
    configurationTest.handleRequest(
        new ByteArrayInputStream(new byte[]{}),new ByteArrayOutputStream(),context);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContextConfigurationNoWildcard() throws IOException {
    TestConfigurationUtil.purgeSystemEnvironmentVariables();
    System.setProperty(ConfigurationInitializer.CFG_SQS_QUEUE,"dummyQ");
    System.setProperty(ConfigurationInitializer.CFG_S3_BUCKET_NAME,"dummyBucket");

    System.setProperty(ConfigurationInitializer.CFG_S3_FILE_PATTERN,"abc");

    Context context = TestConfigurationUtil.initContext(log,100);

    ConfigurationTest configurationTest = new ConfigurationTest();
    configurationTest.handleRequest(
        new ByteArrayInputStream(new byte[]{}),new ByteArrayOutputStream(),context);
  }

  @Test
  public void testContextConfiguration() throws IOException {

    TestConfigurationUtil.loadLocalTestConfiguration(log);

    Context context = TestConfigurationUtil.initContext(log,20000);

    ConfigurationTest configurationTest = new ConfigurationTest();
    configurationTest.handleRequest(
        new ByteArrayInputStream(new byte[]{}),new ByteArrayOutputStream(),context);
  }


}
