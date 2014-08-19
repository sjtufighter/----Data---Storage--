package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;
/*
 * adapted  from Parquet*
 */


//import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * Reads values that have been dictionary encoded
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private ByteArrayInputStream in;

  private Dictionary dictionary;

  private RunLengthBitPackingHybridDecoder decoder;

  public DictionaryValuesReader(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public void initFromPage(int valueCount, byte[] page, int offset)
      throws IOException {
    if (Log.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (page.length - offset));
    this.in = new ByteArrayInputStream(page, offset, page.length - offset);
    int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
    if (Log.DEBUG) LOG.debug("bit width " + bitWidth);
    decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
  }

  @Override
  public int readValueDictionaryId() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public Binary readBytes() {
    try {
      return dictionary.decodeToBinary(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }
  public byte readByte() throws IOException {
	 //   try {
	      return dictionary.decodeToBinary(decoder.readInt()).getBytes()[0];
	 //   } catch (IOException e) {
	  //    throw new ParquetDecodingException(e);
	  //  }
	  }

  @Override
  public float readFloat() {
    try {
      return dictionary.decodeToFloat(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return dictionary.decodeToDouble(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public int readInteger() {
    try {
    	int id=decoder.readInt();
      return dictionary.decodeToInt(id);
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      return dictionary.decodeToLong(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public void skip() {
    try {
      decoder.readInt(); // Type does not matter as we are just skipping dictionary keys
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }
}