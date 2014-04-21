package FlexibleEncoding.ORC;
/**
adapted from ORC
@author wangmeng
 */


/**
 * An interface for recording positions in a stream.
 */
public interface PositionRecorder {
  void addPosition(long offset);
}