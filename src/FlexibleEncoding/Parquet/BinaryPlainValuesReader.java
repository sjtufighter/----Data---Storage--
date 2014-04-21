package FlexibleEncoding.Parquet;
/*
 * adapt from  parquet

 */
//

import java.io.IOException;


public class BinaryPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BinaryPlainValuesReader.class);
  private byte[] in;
  private int offset;

  @Override
  public Binary readBytes() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      int start = offset + 4;
      offset = start + length;
      return Binary.fromByteArray(in, start, length);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not read bytes at offset " + offset, e);
    }
  }

  @Override
  public void skip() {
    try {
      int length = BytesUtils.readIntLittleEndian(in, offset);
      offset += 4 + length;
    } catch (IOException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    } catch (RuntimeException e) {
      throw new ParquetDecodingException("could not skip bytes at offset " + offset, e);
    }
  }

  @Override
  public void initFromPage(int valueCount, byte[] in, int offset)
      throws IOException {
    if (Log.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in = in;
    this.offset = offset;
  }

}