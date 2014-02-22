package DeltaBinaryPacking;

/*
 * adapted  from Parquet*
 */



public interface IntPackerFactory {

  IntPacker newIntPacker(int width);

}
