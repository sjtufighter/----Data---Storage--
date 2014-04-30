package org.apache.hadoop.hive.mastiffFlexibleEncoding.orc;

/**
adapted from ORC
@author wangmeng
 */

import java.io.IOException;
import java.nio.ByteBuffer;

interface CompressionCodec {
  /**
   * Compress the in buffer to the out buffer.
   * @param in the bytes to compress
   * @param out the uncompressed bytes
   * @param overflow put any additional bytes here
   * @return true if the output is smaller than input
   * @throws IOException
   */
  boolean compress(ByteBuffer in, ByteBuffer out, ByteBuffer overflow
                  ) throws IOException;

  /**
   * Decompress the in buffer to the out buffer.
   * @param in the bytes to decompress
   * @param out the decompressed bytes
   * @throws IOException
   */
  void decompress(ByteBuffer in, ByteBuffer out) throws IOException;
}