package DeltaBinaryPacking;

/*
 * adapt from  parquet
 *
 */
public interface ColumnReader {

  /**
   * @return the totalCount of values to be consumed
   */
  long getTotalValueCount();

  /**
   * Consume the current triplet, moving to the next value.
   */
  void consume();

  /**
   * must return 0 when isFullyConsumed() == true
   * @return the repetition level for the current value
   */
  int getCurrentRepetitionLevel();

  /**
   * @return the definition level for the current value
   */
  int getCurrentDefinitionLevel();

  /**
   * writes the current value to the converter
   */
  void writeCurrentValueToConverter();

  /**
   * Skip the current value
   */
  void skip();

  /**
   * available when the underlying encoding is dictionary based
   * @return the dictionary id for the current value
   */
  int getCurrentValueDictionaryID();

  /**
   * @return the current value
   */
  int getInteger();

  /**
   * @return the current value
   */
  boolean getBoolean();

  /**
   * @return the current value
   */
  long getLong();

  /**
   * @return the current value
   */
  Binary getBinary();

  /**
   * @return the current value
   */
  float getFloat();

  /**
   * @return the current value
   */
  double getDouble();

  /**
   * @return Descriptor of the column.
   */
  ColumnDescriptor getDescriptor();

}