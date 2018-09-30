package com.amazon.kinesis.kafka;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author nehalmeh
 */
public class FirehoseSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(FirehoseSinkTask.class);

    private String deliveryStreamName;

    private AmazonKinesisFirehose firehoseClient;

    private boolean batch;

    private int batchSize;

    private int batchSizeInBytes;

    @Override
    public String version() {
        return new FirehoseSinkConnector().version();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> arg0) {
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        if (batch)
            putRecordsInBatch(sinkRecords);
        else
            putRecords(sinkRecords);
    }

    @Override
    public void start(Map<String, String> props) {
        batch = Boolean.parseBoolean(props.get(FirehoseSinkConnector.BATCH));
        batchSize = Integer.parseInt(props.get(FirehoseSinkConnector.BATCH_SIZE));
        batchSizeInBytes = Integer.parseInt(props.get(FirehoseSinkConnector.BATCH_SIZE_IN_BYTES));
        deliveryStreamName = props.get(FirehoseSinkConnector.DELIVERY_STREAM);
        final AmazonKinesisFirehoseClientBuilder builder = AmazonKinesisFirehoseClient.builder();
        builder.setCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        builder.setRegion(props.get(FirehoseSinkConnector.REGION));
        firehoseClient = builder.build();

        // Validate delivery stream
        validateDeliveryStream();
    }

    @Override
    public void stop() {

    }


    /**
     * Validates status of given Amazon Kinesis Firehose Delivery Stream.
     */
    private void validateDeliveryStream() {
        DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
        describeDeliveryStreamRequest.setDeliveryStreamName(deliveryStreamName);
        DescribeDeliveryStreamResult describeDeliveryStreamResult = firehoseClient
                .describeDeliveryStream(describeDeliveryStreamRequest);
        if (!describeDeliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus().equals("ACTIVE")) {
            log.error("Connector can't start to do inactive delivery stream.");
            throw new ConfigException("Connecter cannot start as configured delivery stream is not active"
                    + describeDeliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus());
        }
    }

    /**
     * Method to perform PutRecordBatch operation with the given record list.
     *
     * @param recordList the collection of records
     * @return the output of PutRecordBatch
     */
    private PutRecordBatchResult putRecordBatch(List<Record> recordList) {
        PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
        putRecordBatchRequest.setDeliveryStreamName(deliveryStreamName);
        putRecordBatchRequest.setRecords(recordList);

        int retries = 10;
        int waitTime = 1000;
        // waitTimes: 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000
        // Put Record Batch records. Max No.Of Records we can put in a
        // single put record batch request is 500 and total size < 4MB
        PutRecordBatchResult putRecordBatchResult = null;
        while (retries > 0) {
            try {
                putRecordBatchResult = firehoseClient.putRecordBatch(putRecordBatchRequest);
                int failures = putRecordBatchResult.getFailedPutCount();
                if (failures > 0) {
                    log.error("Amazon Kinesis Firehose Fail:" + putRecordBatchResult.toString());
                } else {
                    retries = 0;
                }
            } catch (AmazonKinesisFirehoseException akfe) {
                log.error("Amazon Kinesis Firehose Exception:" + akfe.getLocalizedMessage());
                log.error(String.format("Waiting: %d ms and retrying.  Retries remaining %d. ", waitTime, retries));

            } catch (Exception e) {
                log.error("Connector Exception" + e.getLocalizedMessage());
                // something really bad happened, don't retry
                retries = 0;
            }
            if (retries > 0) {
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                waitTime *= 2; // exponential fall off
                retries--;
            } else {
                retries = 0;
            }
        }
        return putRecordBatchResult;
    }

    private void putRecordsInBatch(Collection<SinkRecord> sinkRecords) {
        List<Record> recordList = new ArrayList<Record>();
        int recordsInBatch = 0;
        int recordsSizeInBytes = 0;

        for (SinkRecord sinkRecord : sinkRecords) {
            Record record = DataUtility.createFirehoseRecord(sinkRecord);
            recordList.add(record);
            recordsInBatch++;
            recordsSizeInBytes += record.getData().capacity();

            if (recordsInBatch == batchSize || recordsSizeInBytes > batchSizeInBytes) {
                putRecordBatch(recordList);
                recordList.clear();
                recordsInBatch = 0;
                recordsSizeInBytes = 0;
            }
        }

        if (recordsInBatch > 0) {
            putRecordBatch(recordList);
        }
    }


    private void putRecords(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord sinkRecord : sinkRecords) {

            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setDeliveryStreamName(deliveryStreamName);
            putRecordRequest.setRecord(DataUtility.createFirehoseRecord(sinkRecord));

            int retries = 10;
            int waitTime = 1000;
            // waitTimes: 1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000
            PutRecordResult putRecordResult;
            while (retries > 0) {
                try {
                    firehoseClient.putRecord(putRecordRequest);
                } catch (AmazonKinesisFirehoseException akfe) {
                    log.error("Amazon Kinesis Firehose Exception:" + akfe.getLocalizedMessage());
                    log.error(String.format("Waiting: %d ms and retrying.  Retries remaining %d. ", waitTime, retries));
                    // we may be getting rate limitted
                    try {
                        Thread.sleep(waitTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    waitTime *= 2; // exponential fall off
                    retries--;
                } catch (Exception e) {
                    log.error("Connector Exception" + e.getLocalizedMessage());
                }
            }
        }
    }

}
