## AWS Lambda Java Transfer SQS messages to S3
This library can be used for transferring messages from SQS to S3 files.


## Configuration
The .jar must be built and uploaded to AWS using the AWS Console.

The environment variables "SQS_Source_Queue" and "S3_Bucket_Name" must be
configured. Other variables can configured. See [`ConfigurationInitializer`](src/main/java/com/udoheld/aws/lambda/sqs/to/s3/cfg/ConfigurationInitializer.java) for a list of all variables.

Please give this Lambda enough Memory and Runtime.

A minimum of 256 MB is recommended.
As the AWS SDK is quite large the application is likely to take more than 15
seconds for the first initialization. For larger batches it is a good idea to
give it the maximum of 5 Minutes.

For the queue configuration make sure that the visibility time-out is higher
than your Lambda runtime.

Configure a policy for SQS and S3 permissions.

## Handlers
com.udoheld.aws.lambda.sqs.to.s3.cfg.ConfigurationTest lets you test your
configuration.

com.udoheld.aws.lambda.sqs.to.s3.TransferMessagesFromSqsToS3Lambda runs the
actual transfer.