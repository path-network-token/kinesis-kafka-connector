package com.amazon.kinesis.kafka;

import com.amazonaws.services.kinesisfirehose.model.Record;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DataUtility {

    private static final Logger log = LoggerFactory.getLogger(DataUtility.class);

    /**
     * Parses Kafka Values
     *
     * @param schema - Schema of passed message as per
     *                 https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.html
     * @param value  - Value of the message
     * @return Parsed bytebuffer as per schema type
     */
    public static ByteBuffer parseValue(Schema schema, Object value) {
        Schema.Type t = schema.type();
        switch (t) {
            case INT8:
                ByteBuffer byteBuffer = ByteBuffer.allocate(1);
                byteBuffer.put((Byte) value);
                return byteBuffer;
            case INT16:
                ByteBuffer shortBuf = ByteBuffer.allocate(2);
                shortBuf.putShort((Short) value);
                return shortBuf;
            case INT32:
                ByteBuffer intBuf = ByteBuffer.allocate(4);
                intBuf.putInt((Integer) value);
                return intBuf;
            case INT64:
                ByteBuffer longBuf = ByteBuffer.allocate(8);
                longBuf.putLong((Long) value);
                return longBuf;
            case FLOAT32:
                ByteBuffer floatBuf = ByteBuffer.allocate(4);
                floatBuf.putFloat((Float) value);
                return floatBuf;
            case FLOAT64:
                ByteBuffer doubleBuf = ByteBuffer.allocate(8);
                doubleBuf.putDouble((Double) value);
                return doubleBuf;
            case BOOLEAN:
                ByteBuffer boolBuffer = ByteBuffer.allocate(1);
                boolBuffer.put((byte) ((Boolean) value ? 1 : 0));
                return boolBuffer;
            case STRING:
                try {
                    return ByteBuffer.wrap(((String) value).getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    // TODO Auto-generated catch block
                    log.error("Message cannot be translated:" + e.getLocalizedMessage());
                }
            case ARRAY:
                Schema sch = schema.valueSchema();
                if (sch.type() == Type.MAP || sch.type() == Type.STRUCT) {
                    throw new DataException("Invalid schema type.");
                }
                Object[] objs = (Object[]) value;
                ByteBuffer[] byteBuffers = new ByteBuffer[objs.length];
                int noOfByteBuffer = 0;

                for (Object obj : objs) {
                    byteBuffers[noOfByteBuffer++] = parseValue(sch, obj);
                }

                ByteBuffer result = ByteBuffer.allocate(Arrays.stream(byteBuffers).mapToInt(Buffer::remaining).sum());
                Arrays.stream(byteBuffers).forEach(bb -> result.put(bb.duplicate()));
                return result;
            case BYTES:
                if (value instanceof byte[])
                    return ByteBuffer.wrap((byte[]) value);
                else if (value instanceof ByteBuffer)
                    return (ByteBuffer) value;
            case MAP:
                // TO BE IMPLEMENTED
                return ByteBuffer.wrap(null);
            case STRUCT:

                List<ByteBuffer> fieldList = new LinkedList<ByteBuffer>();
                // Parsing each field of structure
                schema.fields().forEach(field -> fieldList.add(parseValue(field.schema(), ((Struct) value).get(field))));
                // Initialize ByteBuffer
                ByteBuffer processedValue = ByteBuffer.allocate(fieldList.stream().mapToInt(Buffer::remaining).sum());
                // Combine bytebuffer of all fields
                fieldList.forEach(buffer -> processedValue.put(buffer.duplicate()));

                return processedValue;

        }
        return null;
    }

    // TODO MKN: fix, use, test
    public static ByteBuffer parseJobResultValue(Schema schema, Object value) {
        // TODO minor MKN: make it generic (not specific to the "JobResult" format)...
        Schema.Type schemaType = schema.type();
        switch (schemaType) {
            case STRUCT:

                // TODO MKN: received_on,customer_uuid,job_uuid,result_uuid,miner_id,asn,ip_range,geo_lat,geo_long,derived_lat,derived_long,derived_accuracy,status,response_time,response_body,kafka_topic,kafka_offset,kafka_partition
                /*
                    received_on
                    result_uuid
                    execution_id
                    job_uuid
                    customer_uuid
                    miner_id
                    wallet_id
                    asn
                    ip_range
                    ip
                    geo_lat
                    geo_long
                    derived_lat
                    derived_long
                    derived_accuracy
                    status
                    response_time
                    response_body
                    kafka_topic
                    kafka_offset
                    kafka_partition
                 */

                List<ByteBuffer> fieldList = new LinkedList<ByteBuffer>();

                // Parsing each field of structure
                schema.fields().forEach(field -> fieldList.add(parseValue(field.schema(), ((Struct) value).get(field))));
                // Initialize ByteBuffer
                ByteBuffer processedValue = ByteBuffer.allocate(fieldList.stream().mapToInt(Buffer::remaining).sum());
                // Combine bytebuffer of all fields
                fieldList.forEach(buffer -> processedValue.put(buffer.duplicate()));

                return processedValue;

        }
        // TODO MKN: log error
        throw new IllegalArgumentException(String.format("Unexpected schema type: %s", schemaType));
    }

    /**
     * Converts Kafka record into Kinesis record
     *
     * @param sinkRecord Kafka unit of message
     * @return Kinesis unit of message
     */
    public static Record createFirehoseRecord(SinkRecord sinkRecord) {
        return new Record().withData(parseJobResultValue(sinkRecord.valueSchema(), sinkRecord.value()));
    }

}
