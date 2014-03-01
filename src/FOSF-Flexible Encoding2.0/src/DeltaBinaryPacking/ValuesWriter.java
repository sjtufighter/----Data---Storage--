package DeltaBinaryPacking;
/*
 * adapted  from Parquet*
 */
/**
 * base class to implement an encoding for a given column
 *
 * @author Julien Le Dem
 *
 */
public abstract class ValuesWriter {

  /**
   * used to decide if we want to work to the next page
   * @return the size of the currently buffered data (in bytes)
   */
  public abstract long getBufferedSize();


  // TODO: maybe consolidate into a getPage
  /**
   * @return the bytes buffered so far to write to the current page
   */
  public abstract BytesInput getBytes();

  /**
   * called after getBytes() and before reset()
   * @return the encoding that was used to encode the bytes
   */
  public abstract Encoding getEncoding();

  /**
   * called after getBytes() to reset the current buffer and start writing the next page
   */
  public abstract void reset();

  /**
   * @return the dictionary page or null if not dictionary based
   */
  public DictionaryPage createDictionaryPage() {
    return null;
  }

  /**
   * reset the dictionary when a new block starts
   */
  public void resetDictionary() {
  }

  /**
   *
   * @return the allocated size of the buffer
   * ( > {@link #getBufferedMemorySize()() )
   */
  abstract public long getAllocatedSize();

  /**
   * @param value the value to encode
   */
  public void writeByte(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeBoolean(boolean v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeBytes(Binary v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeInteger(int v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeLong(long v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeDouble(double v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value the value to encode
   */
  public void writeFloat(float v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  abstract public String memUsageString(String prefix);

}