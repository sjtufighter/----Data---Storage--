package FlexibleEncoding.Parquet;
/*
 * adapted  from Parquet*
 */



/**
 * thrown when an encoding problem occured
 *
 * @author Julien Le Dem
 *
 */
public class ParquetDecodingException extends ParquetRuntimeException {
  private static final long serialVersionUID = 1L;

  public ParquetDecodingException() {
  }

  public ParquetDecodingException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParquetDecodingException(String message) {
    super(message);
  }

  public ParquetDecodingException(Throwable cause) {
    super(cause);
  }

}