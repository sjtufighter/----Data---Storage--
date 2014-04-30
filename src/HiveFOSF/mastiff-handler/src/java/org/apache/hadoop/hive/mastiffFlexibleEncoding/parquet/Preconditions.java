package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */


/**
 * Utility for parameter validation
 *
 * @author Julien Le Dem
 *
 */
public final class Preconditions {
  private Preconditions() { }

  /**
   * @param o the param to check
   * @param name the name of the param for the error message
   * @return the validated o
   * @throws NullPointerException if o is null
   */
  public static <T> T checkNotNull(T o, String name) throws NullPointerException {
    if (o == null) {
      throw new NullPointerException(name + " should not be null");
    }
    return o;
  }

  /**
   * @param valid whether the argument is valid
   * @param message error message if the argument is not valid
   * @throws IllegalArgumentException if !valid
   */
  public static void checkArgument(boolean valid, String message) throws IllegalArgumentException {
    if (!valid) {
      throw new IllegalArgumentException(message);
    }
  }
}
