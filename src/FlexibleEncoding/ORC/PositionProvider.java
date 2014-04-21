package FlexibleEncoding.ORC;
/**
adapted from ORC
@author wangmeng
 */



/**
 * An interface used for seeking to a row index.
 */
public interface PositionProvider {
  long getNext();
}