package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */


//import static parquet.Log.DEBUG;

import java.io.IOException;

/**
 * Reads binary data written by {@link DeltaLengthByteArrayValueWriter}
 *
 * @author Aniket Mokashi
 *
 */
public class DeltaLengthByteArrayValuesReader extends ValuesReader {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesReader.class);
  private ValuesReader lengthReader;
  private byte[] in;
  private int offset;

  public DeltaLengthByteArrayValuesReader() {
    this.lengthReader = new DeltaBinaryPackingValuesReader();
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset)
      throws IOException {
    if (Log.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    lengthReader.initFromPage(valueCount, in, offset);
    offset = lengthReader.getNextOffset();
    this.in = in;
    this.offset = offset;
  }

  @Override
  public Binary readBytes() {
    int length = lengthReader.readInteger();
    int start = offset;
    offset = start + length;
    return Binary.fromByteArray(in, start, length);
  }

  @Override
  public void skip() {
    int length = lengthReader.readInteger();
    offset = offset + length;
  }
}