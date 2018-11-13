package com.l1.kinesis.sample;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class AmazonKinesisApplicationRecordProcessorFactory implements ShardRecordProcessorFactory {
	/**
	 * {@inheritDoc}
	 */
	@Override
	public ShardRecordProcessor shardRecordProcessor() {
		return new AmazonKinesisApplicationSampleRecordProcessor();
	}
}
