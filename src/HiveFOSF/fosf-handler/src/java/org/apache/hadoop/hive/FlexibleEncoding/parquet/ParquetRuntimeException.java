package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */

/**
 * The parent class for all runtime exceptions
 *
 * @author Julien Le Dem
 *
 */
abstract public class ParquetRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public ParquetRuntimeException() {
    super();
  }

  public ParquetRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParquetRuntimeException(String message) {
    super(message);
  }

  public ParquetRuntimeException(Throwable cause) {
    super(cause);
  }

}
