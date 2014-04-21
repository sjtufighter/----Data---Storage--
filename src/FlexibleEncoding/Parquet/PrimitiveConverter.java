package FlexibleEncoding.Parquet;

/*
 * adapted  from Parquet*
 */

/**
 * converter for leaves of the schema
 *
 * @author Julien Le Dem
 *
 */
abstract public class PrimitiveConverter extends Converter {

  public boolean isPrimitive() {
    return true;
  }

  @Override
  public PrimitiveConverter asPrimitiveConverter() {
    return this;
  }

  /**
   * if it returns true we will attempt to use dictionary based conversion instead
   * @return if dictionary is supported
   */
  public boolean hasDictionarySupport() {
    return false;
  }

  /**
   * Set the dictionary to use if the data was encoded using dictionary encoding
   * and the converter hasDictionarySupport().
   * @param dictionary the dictionary to use for conversion
   */
  public void setDictionary(Dictionary dictionary) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /** runtime calls  **/

  /**
   * add a value based on the dictionary set with setDictionary()
   * Will be used if the Converter has dictionary support and the data was encoded using a dictionary
   * @param dictionaryId the id in the dictionary of the value to add
   */
  public void addValueFromDictionary(int dictionaryId) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBinary(Binary value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addBoolean(boolean value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addDouble(double value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addFloat(float value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addInt(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param value value to set
   */
  public void addLong(long value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

}
