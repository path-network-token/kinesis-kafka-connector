package com.amazon.kinesis.kafka;

import au.com.bytecode.opencsv.CSVWriter;
import com.amazonaws.services.kinesisfirehose.model.Record;
import network.path.kafka.connect.PathConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

// TODO MKN: make it enum with "instance" (singleton)
public class DataUtility {

    private static final Logger log = LoggerFactory.getLogger(DataUtility.class);

    // Note: it includes milliseconds
    private static final DateTimeFormatter redshiftTimestampFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd HH:mm:ss.SSS")
            .withZoneUTC();


    /**
     * Converts Kafka record into Kinesis FireHouse record
     *
     * @param sinkRecord Kafka unit of message
     * @return Kinesis unit of message
     */
    public static Record createFirehoseRecord(SinkRecord sinkRecord) {

        final String topic = sinkRecord.topic();
        final Integer partition = sinkRecord.kafkaPartition();
        final long offset = sinkRecord.kafkaOffset();

        return new Record().withData(parseJobResultValue(
                sinkRecord.valueSchema(),
                sinkRecord.value(),
                topic,
                partition,
                offset
        ));
    }

    // TODO MKN: test
    public static ByteBuffer parseJobResultValue(Schema schema, Object value,
                                                 String kafka_topic, Integer kafka_partition, long kafka_offset) {
        // TODO minor MKN: make it generic (not specific to the "JobResult" format)...
        if (schema.type() == Type.STRUCT && schema.name().startsWith(PathConstants.JOBS_RESULT_V_X_SCHEMA_NAME_START)) {
            // Note: received_on,result_uuid,execution_id,job_uuid,customer_uuid,miner_id,wallet_id,asn,ip_range,ip,geo_lat,geo_long,derived_lat,derived_long,derived_accuracy,status,response_time,response_body,kafka_topic,kafka_offset,kafka_partition
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

            final Struct struct = (Struct) value;

            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final OutputStreamWriter outputStreamWriter = new OutputStreamWriter(bos);

            // TODO MKN: revise how CSVWriter is being used
            final CSVWriter csvWriter = new CSVWriter(outputStreamWriter);

            final String[] data = new String[]{
                toRedshiftTimestamp(struct.getInt64("received_on")),
                toRedshiftString(struct.getString("result_uuid")),
                toRedshiftString(struct.getString("execution_id")),
                toRedshiftString(struct.getString("job_uuid")),
                toRedshiftString(struct.getString("customer_uuid")),
                toRedshiftString(struct.getString("miner_id")),
                toRedshiftString(struct.getString("wallet_id")),
                toRedshiftString(struct.getInt64("asn")),
                toRedshiftString(struct.getString("ip_range")),
                toRedshiftString(struct.getString("ip")),
                toRedshiftString(struct.getFloat64("geo_lat")),
                toRedshiftString(struct.getFloat64("geo_long")),
                toRedshiftString(struct.getFloat64("derived_lat")),
                toRedshiftString(struct.getFloat64("derived_long")),
                toRedshiftString(struct.getInt64("derived_accuracy")),
                toRedshiftString(struct.getString("status")),
                toRedshiftString(struct.getInt64("response_time")),
                toRedshiftSafeString(struct.getString("response_body")),
                toRedshiftString(kafka_topic),
                toRedshiftString(kafka_offset),
                toRedshiftString(kafka_partition)
            };

            // Notes:
            // 1) Double quotes are escaped by the CSVWriter with double quotes (needed for Redshift)
            // 2) The ending new line needed for Firehouse is added by the CSVWriter
            csvWriter.writeNext(data);

            try {
                csvWriter.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }

            return ByteBuffer.wrap(bos.toByteArray());

        } else {
            final String errorMessage = String.format("Unexpected schema: %s", schema);
            log.error(errorMessage);
            throw new IllegalArgumentException(errorMessage);
        }
    }

    // TODO MKN: auto-test
    public static String toRedshiftTimestamp(Long nullableTimestamp) {
        return nullableTimestamp == null
                ? PathConstants.REDSHIFT_EMPTY_STRING
                : redshiftTimestampFormatter.print(nullableTimestamp);
    }

    private static String toRedshiftString(String nullableString) {
        return nullableString == null
                ? PathConstants.REDSHIFT_EMPTY_STRING
                : nullableString;
    }

    private static String toRedshiftString(Long nullableLong) {
        return nullableLong == null
                ? PathConstants.REDSHIFT_EMPTY_STRING
                : nullableLong.toString();
    }

    private static String toRedshiftString(Double nullableDouble) {
        return nullableDouble == null
                ? PathConstants.REDSHIFT_EMPTY_STRING
                : BigDecimal.valueOf(nullableDouble).toPlainString();
    }

    private static String toRedshiftString(Integer nullableInteger) {
        return nullableInteger == null
                ? PathConstants.REDSHIFT_EMPTY_STRING
                : nullableInteger.toString();
    }

    public static String toRedshiftSafeString(String nullableString) {
        if (nullableString == null) {
            return PathConstants.REDSHIFT_EMPTY_STRING;
        }

        // TODO MKN: impl; see "makeSafeForRedshiftCopy" in "kafka" repo
        String str = nullableString.trim();


        // Note: replacing all new lines with empty strings (globally, multiline)
        /*
                            str = str.replace(/(\r\n\t|\n|\r\t)/gm, '');
         */

        // Note: see https://stackoverflow.com/questions/9849015/java-regex-using-strings-replaceall-method-to-replace-newlines
        str = str.replaceAll("\\R", PathConstants.REDSHIFT_EMPTY_STRING);

        // Note: replacing all characters other than listed with underscore (globally, multiline)
        /*
                            str = str.replace(
                                  /[^_ a-zA-Z0-9~!@#$%^&*()|+=?;:'",.<>/{}\-\[\]\\]/gm,
                                  '_'
                                );
         */

        str = str.replaceAll("[^_ a-zA-Z0-9~!@#$%^&*()|+=?;:'\",.<>/{}\\-\\[\\]\\\\]", "_");

        // Note: we do not need any double quotes escaping here -- it is taken care of by the calling code.

        return str;
    }

    /**
     * Parses Kafka Values
     *
     * @param schema - Schema of passed message as per
     *               https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Schema.html
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

}
