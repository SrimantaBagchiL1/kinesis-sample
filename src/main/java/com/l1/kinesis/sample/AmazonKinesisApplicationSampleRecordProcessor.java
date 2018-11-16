package com.l1.kinesis.sample;
/*
 * Copyright 2012-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.exceptions.ThrottlingException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationSampleRecordProcessor implements ShardRecordProcessor {

	private static final Logger log = LoggerFactory.getLogger(AmazonKinesisApplicationSampleRecordProcessor.class);
	private static final String SHARD_ID_MDC_KEY = "ShardId";
	private String shardId;

	// Backoff and retry settings
	private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
	private static final int NUM_RETRIES = 10;

	// Checkpoint about once a minute
	private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
	private long nextCheckpointTimeInMillis;

	private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

	/**
	 * Process records performing retries as needed. Skip "poison pill" records.
	 * 
	 * @param records
	 *            Data records to be processed.
	 */
	private void processRecordsWithRetries(List<KinesisClientRecord> records) {
		for (KinesisClientRecord record : records) {
			boolean processedSuccessfully = false;
			for (int i = 0; i < NUM_RETRIES; i++) {
				try {
					//
					// Logic to process record goes here.
					//
					processSingleRecord(record);

					processedSuccessfully = true;
					break;
				} catch (Throwable t) {
					log.warn("Caught throwable while processing record " + record, t);
				}

				// backoff if we encounter an exception.
				try {
					Thread.sleep(BACKOFF_TIME_IN_MILLIS);
				} catch (InterruptedException e) {
					log.debug("Interrupted sleep", e);
				}
			}

			if (!processedSuccessfully) {
				log.error("Couldn't process record " + record + ". Skipping the record.");
			}
		}
	}

	/**
	 * Process a single record.
	 * 
	 * @param record
	 *            The record to be processed.
	 */
	private void processSingleRecord(KinesisClientRecord record) {

		DynamoDbAsyncClient ddb = DynamoDbAsyncClient.create();
		String data = null;
		try {
			// For this app, we interpret the payload as UTF-8 chars.
			data = decoder.decode(record.data()).toString();
			HashMap<String, AttributeValue> item_values =
					new HashMap<String,AttributeValue>();

			item_values.put("id", AttributeValue.builder().s(data).build());
			PutItemRequest request = PutItemRequest.builder()
					.tableName("dev-nova-offers")
					.item(item_values)
					.build();
			System.out.println("The data is" + data);
			ddb.putItem(request);
		} catch (NumberFormatException e) {
			log.info("Record does not match sample record format. Ignoring record with data; " + data);
		} catch (CharacterCodingException e) {
			log.error("Malformed data: " + data, e);
		}
	}

	/**
	 * Checkpoint with retries.
	 * 
	 * @param checkpointer
	 */
	private void checkpoint(RecordProcessorCheckpointer checkpointer) {
		log.info("Checkpointing shard " + shardId);
		for (int i = 0; i < NUM_RETRIES; i++) {
			try {
				checkpointer.checkpoint();
				break;
			} catch (ShutdownException se) {
				// Ignore checkpoint if the processor instance has been shutdown
				// (fail over).
				log.info("Caught shutdown exception, skipping checkpoint.", se);
				break;
			} catch (ThrottlingException e) {
				// Backoff and re-attempt checkpoint upon transient failures
				if (i >= (NUM_RETRIES - 1)) {
					log.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
					break;
				} else {
					log.info("Transient issue when checkpointing - attempt " + (i + 1) + " of " + NUM_RETRIES, e);
				}
			} catch (InvalidStateException e) {
				// This indicates an issue with the DynamoDB table (check for
				// table, provisioned IOPS).
				log.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
				break;
			}
			try {
				Thread.sleep(BACKOFF_TIME_IN_MILLIS);
			} catch (InterruptedException e) {
				log.debug("Interrupted sleep", e);
			}
		}
	}

	@Override
	public void initialize(InitializationInput initializationInput) {
		shardId = initializationInput.shardId();
		MDC.put(SHARD_ID_MDC_KEY, shardId);
		try {
			log.info("Initializing @ Sequence: {}", initializationInput.extendedSequenceNumber());
		} finally {
			MDC.remove(SHARD_ID_MDC_KEY);
		}
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {

		MDC.put(SHARD_ID_MDC_KEY, shardId);
		try {
			RecordProcessorCheckpointer checkpointer = processRecordsInput.checkpointer();
			log.info("Processing {} record(s)", processRecordsInput.records().size());
			List<KinesisClientRecord> records = processRecordsInput.records();
			processRecordsWithRetries(records);

			// Checkpoint once every checkpoint interval.
			if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
				checkpoint(checkpointer);
				nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
			}
		} catch (Throwable t) {
			log.error("Caught throwable while processing records.  Aborting");
			Runtime.getRuntime().halt(1);
		} finally {
			MDC.remove(SHARD_ID_MDC_KEY);
		}
	}

	@Override
	public void leaseLost(LeaseLostInput leaseLostInput) {
		MDC.put(SHARD_ID_MDC_KEY, shardId);
		try {
			log.info("Lost lease, so terminating.");
		} finally {
			MDC.remove(SHARD_ID_MDC_KEY);
		}
	}

	@Override
	public void shardEnded(ShardEndedInput shardEndedInput) {
		MDC.put(SHARD_ID_MDC_KEY, shardId);
		try {
			log.info("Reached shard end checkpointing.");
			shardEndedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {
			log.error("Exception while checkpointing at shard end.  Giving up", e);
		} finally {
			MDC.remove(SHARD_ID_MDC_KEY);
		}
	}

	@Override
	public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
		MDC.put(SHARD_ID_MDC_KEY, shardId);
		try {
			log.info("Scheduler is shutting down, checkpointing.");
			shutdownRequestedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {
			log.error("Exception while checkpointing at requested shutdown.  Giving up", e);
		} finally {
			MDC.remove(SHARD_ID_MDC_KEY);
		}
	}
}
