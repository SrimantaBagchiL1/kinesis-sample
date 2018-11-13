package com.l1.kinesis.sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class AmazonKinesisRecordConsumerSample {
	/*
	 * Before running the code: Fill in your AWS access credentials in the
	 * provided credentials file template, and be sure to move the file to the
	 * default location (~/.aws/credentials) where the sample code will load the
	 * credentials from.
	 * https://console.aws.amazon.com/iam/home?#security_credential
	 *
	 * WARNING: To avoid accidental leakage of your credentials, DO NOT keep the
	 * credentials file in your source directory.
	 */

	public static final String SAMPLE_APPLICATION_STREAM_NAME = "TestStreamA";

	private static final String SAMPLE_APPLICATION_NAME = "TestApplicationB";

	private static final Region REGION = Region.US_EAST_1;

	private static final Logger log = LoggerFactory.getLogger(AmazonKinesisRecordConsumerSample.class);

	private static DefaultCredentialsProvider credentialsProvider;

	private static void init() throws Exception {
		// Ensure the JVM will refresh the cached IP values of AWS resources
		// (e.g. service endpoints).
		java.security.Security.setProperty("networkaddress.cache.ttl", "60");

		/*
		 * The ProfileCredentialsProvider will return your [default] credential
		 * profile by reading from the credentials file located at
		 * (~/.aws/credentials).
		 */
		credentialsProvider = DefaultCredentialsProvider.create();
		try {
			credentialsProvider.resolveCredentials();
		} catch (Exception e) {
			throw new Exception("Cannot load the credentials from the credential profiles file. "
					+ "Please make sure that your credentials file is at the correct "
					+ "location (~/.aws/credentials), and is in valid format.", e);
		}
	}

	public static void main(String[] args) throws Exception {

		init();

		String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(REGION).build();
		CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(REGION).build();
		KinesisAsyncClient kinesisClient = KinesisAsyncClient.builder().region(REGION)
				.credentialsProvider(credentialsProvider).build();
		ConfigsBuilder configsBuilder = new ConfigsBuilder(SAMPLE_APPLICATION_STREAM_NAME, SAMPLE_APPLICATION_NAME,
				kinesisClient, dynamoClient, cloudWatchClient, workerId,
				new AmazonKinesisApplicationRecordProcessorFactory());
		Scheduler scheduler = new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
				configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(),
				configsBuilder.metricsConfig(), configsBuilder.processorConfig(), configsBuilder.retrievalConfig());

		Thread schedulerThread = new Thread(scheduler);
		schedulerThread.setDaemon(true);
		schedulerThread.start();

		System.out.println("Press enter to shutdown");
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		try {
			reader.readLine();
		} catch (IOException ioex) {
			log.error("Caught exception while waiting for confirm.  Shutting down", ioex);
		}

		Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
		log.info("Waiting up to 20 seconds for shutdown to complete.");
		try {
			gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			log.info("Interrupted while waiting for graceful shutdown. Continuing.");
		} catch (ExecutionException e) {
			log.error("Exception while executing graceful shutdown.", e);
		} catch (TimeoutException e) {
			log.error("Timeout while waiting for shutdown.  Scheduler may not have exited.");
		}
		log.info("Completed, shutting down now.");
	}
}
