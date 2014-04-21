package FlexibleEncoding.Parquet;

/*
 * adapted  from Parquet*
 */

/**
 * Packs and unpacks into ints
 *
 * packing unpacking treats:
 *  - 32 values at a time
 *  - bitWidth ints at a time.
 *
 * @author Julien Le Dem
 *
 */
public abstract class IntPacker {

  private final int bitWidth;

  IntPacker(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  /**
   * @return the width in bits used for encoding, also how many ints are packed/unpacked at a time
   */
  public final int getBitWidth() {
    return bitWidth;
  }

  /**
   * pack 32 values from input at inPos into bitWidth ints in output at outPos.
   * nextPosition: inPos += 32; outPos += getBitWidth()
   * @param input the input values
   * @param inPos where to read from in input
   * @param output the output ints
   * @param outPos where to write to in output
   */
  public abstract void pack32Values(int[] input, int inPos, int[] output, int outPos);

  /**
   * unpack bitWidth ints from input at inPos into 32 values in output at outPos.
   * nextPosition: inPos += getBitWidth(); outPos += 32
   * @param input the input int
   * @param inPos where to read from in input
   * @param output the output values
   * @param outPos where to write to in output
   */
  public abstract void unpack32Values(int[] input, int inPos, int[] output, int outPos);

}