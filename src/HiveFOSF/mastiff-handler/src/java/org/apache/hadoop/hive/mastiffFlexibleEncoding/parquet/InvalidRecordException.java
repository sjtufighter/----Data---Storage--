package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */

/**
 * thrown when an invalid record is encountered
 *
 * @author Julien Le Dem
 *
 */
public class InvalidRecordException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public InvalidRecordException() {
    super();
  }

  public InvalidRecordException(String message, Throwable cause) {
    super(message, cause);
  }

  public InvalidRecordException(String message) {
    super(message);
  }

  public InvalidRecordException(Throwable cause) {
    super(cause);
  }

}