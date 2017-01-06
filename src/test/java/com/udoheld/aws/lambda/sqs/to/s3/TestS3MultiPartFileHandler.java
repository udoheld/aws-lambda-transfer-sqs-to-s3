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
import com.udoheld.aws.lambda.sqs.to.s3.cfg.Config;
import com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationInitializer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

import static org.junit.Assert.assertTrue;

/**
 * @author Udo Held.
 */
public class TestS3MultiPartFileHandler {

  private AWSCredentials credentials;
  private Config config;
  private Context context;

  private static final int TEST_PARTS_COUNT = 1;
  private static final int TEST_PART_SIZE_MB = 5;

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
  public void testMultipartUpload(){
    DateTimeFormatter dtf = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
    String fileName = this.getClass().getSimpleName() + ZonedDateTime.now().format(dtf);

    S3MultiPartFileHandler fileHandler =
        S3MultiPartFileHandler.startFileUpload(fileName,config, context.getLogger());
    for (int i=0;i < TEST_PARTS_COUNT; i++){
      fileHandler.uploadPart(getTestMessage(i).getBytes());
    }
    assertTrue(fileHandler.finalizeMultipartUpload());
  }


  private String getTestMessage(int partNumber){
    String message = "Part " + partNumber + System.lineSeparator()
        + "abcdefghijklmnopqrstuvwxyz01234567890ABCDEFGHIJKLMNOPQRSTUVW~`!@#$%^&*()_-+={}[]\\|"
        + ":;\"',./<>?¡²³¤€¼½¾‘’äåé®þüúíóö«»¬áßðø¶´æ©ñµç¿¹£÷ÄÅÉÞÜÚÍÓÖ¦Á§ÐØ°¨Æ¢ÑÇ";
    return message;

  }
}
