package DeltaBinaryPacking;

/*
 * adapted  from Parquet*
 */
/**
 * The different type of values we can store in columns
 *
 * @author Julien Le Dem
 *
 */
public enum ValuesType {
  REPETITION_LEVEL, DEFINITION_LEVEL, VALUES;
}