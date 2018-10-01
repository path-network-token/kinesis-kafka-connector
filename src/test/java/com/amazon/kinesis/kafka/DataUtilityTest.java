package com.amazon.kinesis.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class DataUtilityTest {

    @Test
    public void toRedshiftTimestampTest(){
        // TODO MKN: complete this auto-test

        /*

            new Date(1530462533202)
            .toISOString()
            .substr(0, 23)
            .replace('T', ' ')
            ```
            returns `2018-07-01 16:28:53.202`

         */

        final String actual_01 = DataUtility.toRedshiftTimestamp(1530462533202L);
        System.out.println("actual_01 = " + actual_01);
    }

    @Test
    public void toRedshiftSafeStringTest(){
        // TODO MKN: complete this auto-test

        String actual_01 = DataUtility.toRedshiftSafeString("new@test@id_12");
        System.out.println("actual_01 = " + actual_01);

        final String bigJson = "{\n" +
                "    \"Records\": [\n" +
                "      {\n" +
                "        \"eventID\": \"1f5e4cab53002b43a13520e4298dcf51\",\n" +
                "        \"eventName\": \"INSERT\",\n" +
                "        \"eventVersion\": \"1.1\",\n" +
                "        \"eventSource\": \"aws:dynamodb\",\n" +
                "        \"awsRegion\": \"us-west-2\",\n" +
                "        \"dynamodb\": {\n" +
                "          \"ApproximateCreationDateTime\": 1532758200,\n" +
                "          \"Keys\": {\n" +
                "            \"job_cust\": {\n" +
                "              \"S\": \"1e111b01-1646-4712-95e6-39fbedfba5f1_michael@path.network\"\n" +
                "            },\n" +
                "            \"rcvd_rslt\": {\n" +
                "              \"S\": \"1530462533202_81c6d653-003c-4307-be45-feebcad6364p\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"NewImage\": {\n" +
                "            \"ip_range\": {\n" +
                "              \"S\": \"8.145.0.0/16\"\n" +
                "            }\n" +
                "          },\n" +
                "          \"eventSourceARN\": \"arn:aws:dynamodb:us-west-2:325710721840:table/results/stream/2018-07-28T04:45:25.925\"\n" +
                "        }\n" +
                "      }\n" +
                "    ]\n" +
                "  }";

        String actual_02 = DataUtility.toRedshiftSafeString(bigJson);
        System.out.println("actual_02 = " + actual_02);

        String actual_03 = DataUtility.toRedshiftSafeString("xo \b mo");
        System.out.println("actual_03 = " + actual_03);
    }

	@Test
	public void parseInt8ValueTest(){
	
		Schema schema = SchemaBuilder.int8();
		ByteBuffer actual = DataUtility.parseValue(schema, (byte) 2);
		ByteBuffer expected = ByteBuffer.allocate(1).put((byte) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseShortValueTest(){
		
		Schema schema = SchemaBuilder.int16();
		ByteBuffer actual = DataUtility.parseValue(schema, (short) 2);
		ByteBuffer expected = ByteBuffer.allocate(2).putShort((short) 2);
		
		Assert.assertTrue(actual.equals(expected));
	
	}
	
	@Test
	public void parseIntValueTest(){
		
		Schema schemaInt32 = SchemaBuilder.int32();
		ByteBuffer actualInt32 = DataUtility.parseValue(schemaInt32, (int) 2);
		ByteBuffer expectedInt32 = ByteBuffer.allocate(4).putInt( (int) 2);
		
		Assert.assertTrue(actualInt32.equals(expectedInt32));
	}
	
	@Test
	public void  parseLongValueTest(){
		
		Schema schema = SchemaBuilder.int64();
		ByteBuffer actual = DataUtility.parseValue(schema, (long) 2);
		ByteBuffer expected = ByteBuffer.allocate(8).putLong( (long) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseFloatValueTest(){
	
		Schema schema = SchemaBuilder.float32();
		ByteBuffer actual = DataUtility.parseValue(schema, (float) 2);
		ByteBuffer expected = ByteBuffer.allocate(4).putFloat((float) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseDoubleValueTest(){
	
		Schema schema = SchemaBuilder.float64();
		ByteBuffer actual = DataUtility.parseValue(schema, (double) 2);
		ByteBuffer expected = ByteBuffer.allocate(8).putDouble((double) 2);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseBooleanValueTest(){
		
		Schema schema = SchemaBuilder.bool();
		ByteBuffer actual = DataUtility.parseValue(schema, (boolean) true);
		ByteBuffer expected = ByteBuffer.allocate(1).put( (byte) 1);
		
		Assert.assertTrue(actual.equals(expected));
	}
	
	@Test
	public void parseStringValueTest(){
		
		Schema schema = SchemaBuilder.string();
		ByteBuffer actual = DataUtility.parseValue(schema, "Testing Kinesis-Kafka Connector");
		ByteBuffer expected = null;
		try {
			expected = ByteBuffer.wrap(((String) "Testing Kinesis-Kafka Connector").getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Assert.assertTrue(actual.equals(expected));

	}
	
	@Test
	public void parseArrayValueTest(){
		
		Schema valueSchema = SchemaBuilder.int8(); 
		Schema schema = SchemaBuilder.array(valueSchema);
		
		Object[] arrayOfInt8 = { (byte) 1, (byte) 2, (byte) 3, (byte) 4};
		ByteBuffer actual = DataUtility.parseValue(schema, arrayOfInt8);
		ByteBuffer expected = ByteBuffer.allocate(4).put((byte) 1).put( (byte) 2).put( (byte) 3).put( (byte) 4);
		
		Assert.assertTrue(actual.equals(expected));
		
	}
	
	@Test
	public void parseByteValueTest(){
		
		Schema schema = SchemaBuilder.bytes();
		
		byte[] value = "Kinesis-Kafka Connector".getBytes();
		ByteBuffer actual = DataUtility.parseValue(schema, value);
		ByteBuffer expected = ByteBuffer.wrap("Kinesis-Kafka Connector".getBytes());
		
		Assert.assertTrue(actual.equals(expected));
	}

}
