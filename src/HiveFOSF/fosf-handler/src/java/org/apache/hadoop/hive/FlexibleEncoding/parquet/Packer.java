package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;

/*
 * adapted  from Parquet*
 */


/**
 * Factory for packing implementations
 *
 * @author Julien Le Dem
 *
 */
public enum Packer {

  /**
   * packers who fill the Least Significant Bit First
   * int and byte packer have the same result on Big Endian architectures
   */
  BIG_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return beIntPackerFactory.newIntPacker(width);
    }
    @Override
    public BytePacker newBytePacker(int width) {
      return beBytePackerFactory.newBytePacker(width);
    }
  },

  /**
   * packers who fill the Most Significant Bit first
   * int and byte packer have the same result on Little Endian architectures
   */
  LITTLE_ENDIAN {
    @Override
    public IntPacker newIntPacker(int width) {
      return leIntPackerFactory.newIntPacker(width);
    }
    @Override
    public BytePacker newBytePacker(int width) {
      return leBytePackerFactory.newBytePacker(width);
    }
  };

  private static IntPackerFactory getIntPackerFactory(String name) {
 //   return (IntPackerFactory)getStaticField("DeltaBinaryPacking." + name, "factory");
    return (IntPackerFactory)getStaticField("org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet."+name, "factory");
  }

  private static BytePackerFactory getBytePackerFactory(String name) {
    //return (BytePackerFactory)getStaticField("parquet.column.values.bitpacking." + name, "factory");
	//  return (BytePackerFactory)getStaticField( "DeltaBinaryPacking."+name, "factory");
    return (BytePackerFactory)getStaticField("org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet."+name, "factory");
  }

  private static Object getStaticField(String className, String fieldName) {
    try {
      return Class.forName(className).getField(fieldName).get(null);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  static BytePackerFactory beBytePackerFactory = getBytePackerFactory("ByteBitPackingBE");
  static IntPackerFactory beIntPackerFactory = getIntPackerFactory("LemireBitPackingBE");
  static BytePackerFactory leBytePackerFactory = getBytePackerFactory("ByteBitPackingLE");
  static IntPackerFactory leIntPackerFactory = getIntPackerFactory("LemireBitPackingLE");

  /**
   * @param width the width in bits of the packed values
   * @return an int based packer
   */
  public abstract IntPacker newIntPacker(int width);

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer
   */
  public abstract BytePacker newBytePacker(int width);
}