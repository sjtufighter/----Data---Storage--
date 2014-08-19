package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */

import java.io.IOException;



/**
 * ValuesReader for FIXED_LEN_BYTE_ARRAY.
 *
 * @author David Z. Chen <dchen@linkedin.com>
 */
public class FixedLenByteArrayPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(FixedLenByteArrayPlainValuesReader.class);
  private byte[] in;
  private int offset;
  private int length;

  public FixedLenByteArrayPlainValuesReader(int length) {
    this.length = length;
  }

  @Override
  public Binary readBytes() {
    try {
      int start = offset;
      offset = start + length;
      return Binary.fromByteArray(in, start, length);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    offset += length;
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset)
      throws IOException {
    if (Log.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
  }
}
