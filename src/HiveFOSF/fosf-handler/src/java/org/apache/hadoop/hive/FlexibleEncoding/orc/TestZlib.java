package org.apache.hadoop.hive.mastiffFlexibleEncoding.orc;

/**
adapted from ORC
@author wangmeng
 */


import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

public class TestZlib {

  @Test
  public static void testNoOverflow() throws Exception {
    ByteBuffer in = ByteBuffer.allocate(10);
    ByteBuffer out = ByteBuffer.allocate(10);
    in.put(new byte[]{1,2,3,4,5,6,7,10});
    in.flip();
    CompressionCodec codec = new ZlibCodec();
    assertEquals(false, codec.compress(in, out, null));
  }

  @Test
  public static void testCorrupt() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(1000);
    buf.put(new byte[]{127,-128,0,99,98,-1});
    buf.flip();
    CompressionCodec codec = new ZlibCodec();
    ByteBuffer out = ByteBuffer.allocate(1000);
    try {
      codec.decompress(buf, out);
      fail();
    } catch (IOException ioe) {
      // EXPECTED
    }
  }


 public static void  main(String[] args) throws Exception{
   testNoOverflow();
   testCorrupt();
   System.out.println("run  over");


 }
}
