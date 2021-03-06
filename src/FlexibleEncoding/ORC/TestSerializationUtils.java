package FlexibleEncoding.ORC;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.math.BigInteger;

//import org.apache.hadoop.hive.ql.io.orc.SerializationUtils;
//import org.apache.hadoop.hive.ql.io.orc.TestSerializationUtils;
import org.junit.Test;

public class TestSerializationUtils {

	  private InputStream fromBuffer(ByteArrayOutputStream buffer) {
	    return new ByteArrayInputStream(buffer.toByteArray());
	  }

	  @Test
	  public void testDoubles() throws Exception {
	    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	    SerializationUtils.writeDouble(buffer, 1343822337.759);
	    assertEquals(1343822337.759,
	        SerializationUtils.readDouble(fromBuffer(buffer)), 0.0001);
	  }

	  @Test
	  public void testBigIntegers() throws Exception {
	    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(0));
	    assertArrayEquals(new byte[]{0}, buffer.toByteArray());
	    assertEquals(0L,
	        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(1));
	    assertArrayEquals(new byte[]{2}, buffer.toByteArray());
	    assertEquals(1L,
	        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-1));
	    assertArrayEquals(new byte[]{1}, buffer.toByteArray());
	    assertEquals(-1L,
	        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(50));
	    assertArrayEquals(new byte[]{100}, buffer.toByteArray());
	    assertEquals(50L,
	        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(-50));
	    assertArrayEquals(new byte[]{99}, buffer.toByteArray());
	    assertEquals(-50L,
	        SerializationUtils.readBigInteger(fromBuffer(buffer)).longValue());
	    for(int i=-8192; i < 8192; ++i) {
	      buffer.reset();
	        SerializationUtils.writeBigInteger(buffer, BigInteger.valueOf(i));
	      assertEquals("compare length for " + i,
	            i >= -64 && i < 64 ? 1 : 2, buffer.size());
	      assertEquals("compare result for " + i,
	          i, SerializationUtils.readBigInteger(fromBuffer(buffer)).intValue());
	    }
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer,
	        new BigInteger("123456789abcdef0",16));
	    assertEquals(new BigInteger("123456789abcdef0",16),
	        SerializationUtils.readBigInteger(fromBuffer(buffer)));
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer,
	        new BigInteger("-123456789abcdef0",16));
	    assertEquals(new BigInteger("-123456789abcdef0",16),
	        SerializationUtils.readBigInteger(fromBuffer(buffer)));
	    StringBuilder buf = new StringBuilder();
	    for(int i=0; i < 256; ++i) {
	      String num = Integer.toHexString(i);
	      if (num.length() == 1) {
	        buf.append('0');
	      }
	      buf.append(num);
	    }
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer,
	        new BigInteger(buf.toString(),16));
	    assertEquals(new BigInteger(buf.toString(),16),
	        SerializationUtils.readBigInteger(fromBuffer(buffer)));
	    buffer.reset();
	    SerializationUtils.writeBigInteger(buffer,
	        new BigInteger("ff000000000000000000000000000000000000000000ff",16));
	    assertEquals(
	        new BigInteger("ff000000000000000000000000000000000000000000ff",16),
	        SerializationUtils.readBigInteger(fromBuffer(buffer)));
	  }

	  public static void main(String[] args) throws Exception {
	    TestSerializationUtils test = new TestSerializationUtils();
	    test.testDoubles();
	    test.testBigIntegers();
	  }
	}
