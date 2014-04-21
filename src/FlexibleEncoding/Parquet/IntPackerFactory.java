package FlexibleEncoding.Parquet;

/*
 * adapted  from Parquet*
 */



public interface IntPackerFactory {

  IntPacker newIntPacker(int width);

}
