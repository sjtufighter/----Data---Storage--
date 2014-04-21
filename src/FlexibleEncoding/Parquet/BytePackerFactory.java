package FlexibleEncoding.Parquet;
/*
 * adapt from  parquet
 *
 */

public interface BytePackerFactory {

  BytePacker newBytePacker(int width);

}
