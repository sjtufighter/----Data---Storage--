package DeltaBinaryPacking;

/*
 * adapt from  parquet
 *
 */

import java.util.Arrays;



/**
 * Describes a column's type as well as its position in its containing schema.
 *
 * @author Julien Le Dem
 *
 */
public class ColumnDescriptor implements Comparable<ColumnDescriptor> {

  private final String[] path;
  private final PrimitiveType.PrimitiveTypeName type;
  private final int typeLength;
  private final int maxRep;
  private final int maxDef;

  /**
   *
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   */
  public ColumnDescriptor(String[] path, PrimitiveType.PrimitiveTypeName type, int maxRep, 
                          int maxDef) {
    this(path, type, 0, maxRep, maxDef);
  }

  /**
   *
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   * @param maxRep the maximum repetition level for that path
   * @param maxDef the maximum definition level for that path
   */
  public ColumnDescriptor(String[] path, PrimitiveType.PrimitiveTypeName type, 
                          int typeLength, int maxRep, int maxDef) {
    super();
    this.path = path;
    this.type = type;
    this.typeLength = typeLength;
    this.maxRep = maxRep;
    this.maxDef = maxDef;
  }

  /**
   * @return the path to the leaf field in the schema
   */
  public String[] getPath() {
    return path;
  }

  /**
   * @return the maximum repetition level for that path
   */
  public int getMaxRepetitionLevel() {
    return maxRep;
  }

  /**
   * @return  the maximum definition level for that path
   */
  public int getMaxDefinitionLevel() {
    return maxDef;
  }

  /**
   * @return the type of that column
   */
  public PrimitiveType.PrimitiveTypeName getType() {
    return type;
  }

  /**
   * @return the size of the type
   **/
  public int getTypeLength() {
    return typeLength;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(path);
  }

  @Override
  public boolean equals(Object obj) {
    return Arrays.equals(path, ((ColumnDescriptor)obj).path);
  }

  @Override
  public int compareTo(ColumnDescriptor o) {
    // TODO(julien): this will fail if o.path.length < this.path.length
    for (int i = 0; i < path.length; i++) {
      int compareTo = path[i].compareTo(o.path[i]);
      if (compareTo != 0) {
        return compareTo;
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return Arrays.toString(path) + " " + type;
  }
}