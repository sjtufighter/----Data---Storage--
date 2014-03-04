package DeltaBinaryPacking;

/*
 * adapted  from Parquet*
 */


/**
 * thrown when a decoding problem occured
 *
 * @author Julien Le Dem
 *
 */
public class ParquetEncodingException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public ParquetEncodingException() {
  }

  public ParquetEncodingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParquetEncodingException(String message) {
    super(message);
  }

  public ParquetEncodingException(Throwable cause) {
    super(cause);
  }

}