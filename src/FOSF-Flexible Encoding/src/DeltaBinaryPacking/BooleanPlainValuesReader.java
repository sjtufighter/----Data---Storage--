package DeltaBinaryPacking;

/*
 * adapt from  parquet

 */
import java.io.IOException;


/**
 * encodes boolean for the plain encoding: one bit at a time (0 = false)
 *
 * @author Julien Le Dem
 *
 */
public class BooleanPlainValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(BooleanPlainValuesReader.class);

  private ByteBitPackingValuesReader in = new ByteBitPackingValuesReader(1, Packer.LITTLE_ENDIAN);

  /**
   *
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#readBoolean()
   */
  @Override
  public boolean readBoolean() {
    return in.readInteger() == 0 ? false : true;
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#skipBoolean()
   */
  @Override
  public void skip() {
    in.readInteger();
  }

  /**
   * {@inheritDoc}
   * @see parquet.column.values.ValuesReader#initFromPage(byte[], int)
   */
  @Override
  public void initFromPage(int valueCount, byte[] in, int offset) throws IOException {
    if (Log.DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (in.length - offset));
    this.in.initFromPage(valueCount, in, offset);
  }
  
  @Override
  public int getNextOffset() {
    return this.in.getNextOffset();
  }

}