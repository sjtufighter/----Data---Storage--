package DeltaBinaryPacking;
/*
 * adapt from  parquet
 *
 */

public interface BytePackerFactory {

  BytePacker newBytePacker(int width);

}
