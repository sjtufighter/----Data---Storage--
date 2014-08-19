package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;
/*
 * adapted  from Parquet*
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;


/**
 * This ValuesReader does all the reading in {@link #initFromPage}
 * and stores the values in an in memory buffer, which is less than ideal.
 *
 * @author Alex Levenson
 */
public class RunLengthBitPackingHybridValuesReader extends ValuesReader {
  private final int bitWidth;
  private RunLengthBitPackingHybridDecoder decoder;
  private int nextOffset;

  public RunLengthBitPackingHybridValuesReader(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  @Override
  public void initFromPage(int valueCountL, byte[] page, int offset) throws IOException {
    ByteArrayInputStream in = new ByteArrayInputStream(page, offset, page.length - offset);
    int length = BytesUtils.readIntLittleEndian(in);

    decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);

    // 4 is for the length which is stored as 4 bytes little endian
    this.nextOffset = offset + length + 4;
  }
  
  @Override
  public int getNextOffset() {
    return this.nextOffset;
  }

  @Override
  public int readInteger() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }
  
  @Override
  public boolean readBoolean() {
    return readInteger() == 0 ? false : true;
  }

  @Override
  public void skip() {
    readInteger();
  }
}