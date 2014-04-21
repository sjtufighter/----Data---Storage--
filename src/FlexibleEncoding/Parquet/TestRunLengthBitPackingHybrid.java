package FlexibleEncoding.Parquet;


import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Test;
public class TestRunLengthBitPackingHybrid {
/**
 * @author Alex Levenson
 */

  
  @Test
  public void testRLEOnly() throws Exception {
    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(3, 5);
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(4);
    }
    for (int i = 0; i < 100; i++) {
      encoder.writeInt(5);
    }

    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 4
    assertEquals(4, BytesUtils.readIntLittleEndianOnOneByte(is));

    // header = 100 << 1 = 200
    assertEquals(200, BytesUtils.readUnsignedVarInt(is));
    // payload = 5
    assertEquals(5, BytesUtils.readIntLittleEndianOnOneByte(is));

    // end of stream
    assertEquals(-1, is.read());
  }

//    ByteArrayInputStream is = new ByteArrayInputStream(encoder.toBytes().toByteArray());
//
//    // header = 25 << 1 = 50
//    assertEquals(50, BytesUtils.readUnsignedVarInt(is));
//    // payload = 17, stored in 2 bytes
//    assertEquals(17, BytesUtils.readIntLittleEndianOnTwoBytes(is));
//
//    // header = ((16/8) << 1) | 1 = 5
//    assertEquals(5, BytesUtils.readUnsignedVarInt(is));
//    List<Integer> values = unpack(9, 16, is);
//    int v = 0;
//    for (int i = 0; i < 7; i++) {
//      assertEquals(7, (int) values.get(v));
//      v++;
//    }
//
//    assertEquals(8, (int) values.get(v++));
//    assertEquals(9, (int) values.get(v++));
//    assertEquals(10, (int) values.get(v++));
//
//    for (int i = 0; i < 6; i++) {
//      assertEquals(6, (int) values.get(v));
//      v++;
//    }
//
//    // header = 19 << 1 = 38
//    assertEquals(38, BytesUtils.readUnsignedVarInt(is));
//    // payload = 6, stored in 2 bytes
//    assertEquals(6, BytesUtils.readIntLittleEndianOnTwoBytes(is));
//
//    // header = 8 << 1  = 16
//    assertEquals(16, BytesUtils.readUnsignedVarInt(is));
//    // payload = 5, stored in 2 bytes
//    assertEquals(5, BytesUtils.readIntLittleEndianOnTwoBytes(is));
//
//    // end of stream
//    assertEquals(-1, is.read());
//  }

//  public void integrationTest() throws Exception {
//    for (int i = 0; i <= 32; i++) {
//      doIntegrationTest(i);
//    }
//  }

  private static void doIntegrationTest(int bitWidth) throws Exception {
    long modValue = 1L << bitWidth;

    RunLengthBitPackingHybridEncoder encoder = new RunLengthBitPackingHybridEncoder(bitWidth, 1000);
    int numValues = 0;

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (i % modValue));
      
    }
    numValues += 100;

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (77 % modValue));
    }
    numValues += 100;

    for (int i = 0; i < 100; i++) {
      encoder.writeInt((int) (88 % modValue));
    }
    numValues += 100;

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt((int) (i % modValue));
      encoder.writeInt((int) (i % modValue));
      encoder.writeInt((int) (i % modValue));
    }
    numValues += 3000;

    for (int i = 0; i < 1000; i++) {
      encoder.writeInt((int) (17 % modValue));
    }
    numValues += 1000;

    byte[] encodedBytes = encoder.toBytes().toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(encodedBytes);

    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    for (int i = 0; i < 100; i++) {
      assertEquals(i % modValue, decoder.readInt());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(77 % modValue, decoder.readInt());
    }

    for (int i = 0; i < 100; i++) {
      assertEquals(88 % modValue, decoder.readInt());
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(i % modValue, decoder.readInt());
      assertEquals(i % modValue, decoder.readInt());
      assertEquals(i % modValue, decoder.readInt());
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(17 % modValue, decoder.readInt());
    }
  }
  
  
  public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	  doIntegrationTest(1);
	}

}
