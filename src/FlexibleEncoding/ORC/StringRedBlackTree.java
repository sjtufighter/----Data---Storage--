package FlexibleEncoding.ORC;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
//implements Serializable 

//
//import org.apache.hadoop.hive.ql.io.orc.DynamicByteArray;
//import org.apache.hadoop.hive.ql.io.orc.DynamicIntArray;
//import org.apache.hadoop.hive.ql.io.orc.RedBlackTree;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.Visitor;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContext;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContextImpl;
//import org.apache.hadoop.hive.ql.io.orc.DynamicByteArray;
//import org.apache.hadoop.hive.ql.io.orc.DynamicIntArray;
//import org.apache.hadoop.hive.ql.io.orc.RedBlackTree;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.Visitor;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContext;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContextImpl;
//import org.apache.hadoop.hive.ql.io.orc.DynamicByteArray;
//import org.apache.hadoop.hive.ql.io.orc.DynamicIntArray;
//import org.apache.hadoop.hive.ql.io.orc.RedBlackTree;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.Visitor;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContext;
//import org.apache.hadoop.hive.ql.io.orc.StringRedBlackTree.VisitorContextImpl;
import org.apache.hadoop.io.Text;


/**
 * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
 * and an offset for each entry.
 */
public class StringRedBlackTree extends RedBlackTree implements Serializable{
	  private final DynamicByteArray byteArray = new DynamicByteArray();
	  private final DynamicIntArray keyOffsets;
	  private final Text newKey = new Text();

	  public StringRedBlackTree(int initialCapacity) {
	    super(initialCapacity);
	    keyOffsets = new DynamicIntArray(initialCapacity);
	  }

	  public int add(String value) {
	    newKey.set(value);
	    // if the key is new, add it to our byteArray and store the offset & length
	    if (add()) {
	      int len = newKey.getLength();
	      keyOffsets.add(byteArray.add(newKey.getBytes(), 0, len));
	    }
	    return lastAdd;
	  }

	  @Override
	  public  int compareValue(int position) {
	    int start = keyOffsets.get(position);
	    int end;
	    if (position + 1 == keyOffsets.size()) {
	      end = byteArray.size();
	    } else {
	      end = keyOffsets.get(position+1);
	    }
	    return byteArray.compare(newKey.getBytes(), 0, newKey.getLength(),
	                             start, end - start);
	  }

	  /**
	   * The information about each node.
	   */
	  public abstract class  VisitorContext {
	    /**
	     * Get the position where the key was originally added.
	     * @return the number returned by add.
	     */
	   public  abstract int getOriginalPosition();

	    /**
	     * Write the bytes for the string to the given output stream.
	     * @param out the stream to write to.
	     * @throws IOException
	     */
	    public abstract void writeBytes(OutputStream out) throws IOException;

	    /**
	     * Get the original string.
	     * @return the string
	     */
	    public abstract Text getText();

	    /**
	     * Get the number of bytes.
	     * @return the string's length in bytes
	     */
	    public  abstract int getLength();
	  }

	  /**
	   * The interface for visitors.
	   */
	  public interface Visitor {
	    /**
	     * Called once for each node of the tree in sort order.
	     * @param context the information about each node
	     * @throws IOException
	     */
	  public   void visit(VisitorContext context) throws IOException;
	  }

	 public  class VisitorContextImpl  extends VisitorContext implements Serializable {
	    private int originalPosition;
	    private int start;
	    private int end;
	    private final Text text = new Text();

	    public int getOriginalPosition() {
	      return originalPosition;
	    }

	    public Text getText() {
	      byteArray.setText(text, start, end - start);
	      return text;
	    }

	    public void writeBytes(OutputStream out) throws IOException {
	      byteArray.write(out, start, end - start);
	    }

	    public int getLength() {
	      return end - start;
	    }

	    void setPosition(int position) {
	      originalPosition = position;
	      start = keyOffsets.get(originalPosition);
	      if (position + 1 == keyOffsets.size()) {
	        end = byteArray.size();
	      } else {
	        end = keyOffsets.get(originalPosition + 1);
	      }
	    }
	  }

	public  void recurse(int node, Visitor visitor, VisitorContextImpl context
	                      ) throws IOException {
	    if (node != NULL) {
	      recurse(getLeft(node), visitor, context);
	      context.setPosition(node);
	      visitor.visit(context);
	      recurse(getRight(node), visitor, context);
	    }
	  }

	  /**
	   * Visit all of the nodes in the tree in sorted order.
	   * @param visitor the action to be applied to each node
	   * @throws IOException
	   */
	  public void visit(Visitor visitor) throws IOException {
	    recurse(root, visitor, new VisitorContextImpl());
	  }

	  /**
	   * Reset the table to empty.
	   */
	  public void clear() {
	    super.clear();
	    byteArray.clear();
	    keyOffsets.clear();
	  }

	  public void getText(Text result, int originalPosition) {
	    int offset = keyOffsets.get(originalPosition);
	    int length;
	    if (originalPosition + 1 == keyOffsets.size()) {
	      length = byteArray.size() - offset;
	    } else {
	      length = keyOffsets.get(originalPosition + 1) - offset;
	    }
	    byteArray.setText(result, offset, length);
	  }

	  /**
	   * Get the size of the character data in the table.
	   * @return the bytes used by the table
	   */
	  public int getCharacterSize() {
	    return byteArray.size();
	  }

	  /**
	   * Calculate the approximate size in memory.
	   * @return the number of bytes used in storing the tree.
	   */
	  public long getSizeInBytes() {
	    return byteArray.getSizeInBytes() + keyOffsets.getSizeInBytes() +
	      super.getSizeInBytes();
	  }
	}
