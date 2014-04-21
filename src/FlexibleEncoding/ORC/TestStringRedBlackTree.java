package FlexibleEncoding.ORC;
import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;

//
//import org.apache.hadoop.hive.ql.io.orc.DynamicByteArray;
//import org.apache.hadoop.hive.ql.io.orc.InStream;
//import org.apache.hadoop.hive.ql.io.orc.IntegerReader;
//import org.apache.hadoop.hive.ql.io.orc.OrcProto;
//import org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerReader;
//import org.apache.hadoop.hive.ql.io.orc.RunLengthIntegerReaderV2;
//import org.apache.hadoop.hive.ql.io.orc.StreamName;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.junit.Test;
/**
 * Test the red-black tree with string keys.
 */
public class TestStringRedBlackTree { 
	private static  HashMap<Integer,String>   hashMap=new HashMap<Integer,String>() ;
	private ArrayList<Integer>  arrayList=new ArrayList<Integer>() ;
	private static final int INITIAL_DICTIONARY_SIZE = 4096;
	private static  OutStream stringOutput;
	private static  IntegerWriter lengthOutput;
	private IntegerWriter rowOutput;
	public StringRedBlackTree dictionary  =new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
	private boolean isDirectV2 = true;
	private  DynamicIntArray rows = new DynamicIntArray();
	public   int[] dumpOrder ;
	public  int currentId=0;
	public  int dictionarySize=0;
	///////////////  reader
  public DynamicByteArray dictionaryBuffer;
	private int[] dictionaryOffsets;
	private IntegerReader reader;
	static TestInStream.OutputCollector collect1 = new TestInStream.OutputCollector();
	static TestInStream.OutputCollector collect2 = new TestInStream.OutputCollector();
	static TestInStream.OutputCollector collect3 = new TestInStream.OutputCollector();
	/**
	 * Checks the red-black tree rules to make sure that we have correctly built
	 * a valid tree.
	 *
	 * Properties:
	 *   1. Red nodes must have black children
	 *   2. Each node must have the same black height on both sides.
	 *
	 * @param node The id of the root of the subtree to check for the red-black
	 *        tree properties.
	 * @return The black-height of the subtree.
	 */
	private int checkSubtree(RedBlackTree tree, int node, IntWritable count
			) throws IOException {
		if (node == RedBlackTree.NULL) {
			return 1;
		}
		count.set(count.get() + 1);
		boolean is_red = tree.isRed(node);
		int left = tree.getLeft(node);
		int right = tree.getRight(node);
		if (is_red) {
			if (tree.isRed(left)) {
				printTree(tree, "", tree.root);
				throw new IllegalStateException("Left node of " + node + " is " + left +
						" and both are red.");
			}
			if (tree.isRed(right)) {
				printTree(tree, "", tree.root);
				throw new IllegalStateException("Right node of " + node + " is " +
						right + " and both are red.");
			}
		}
		int left_depth = checkSubtree(tree, left, count);
		int right_depth = checkSubtree(tree, right, count);
		if (left_depth != right_depth) {
			printTree(tree, "", tree.root);
			throw new IllegalStateException("Lopsided tree at node " + node +
					" with depths " + left_depth + " and " + right_depth);
		}
		if (is_red) {
			return left_depth;
		} else {
			return left_depth + 1;
		}
	}

	/**
	 * Checks the validity of the entire tree. Also ensures that the number of
	 * nodes visited is the same as the size of the set.
	 */
	void checkTree(RedBlackTree tree) throws IOException {
		IntWritable count = new IntWritable(0);
		if (tree.isRed(tree.root)) {
			printTree(tree, "", tree.root);
			throw new IllegalStateException("root is red");
		}
		checkSubtree(tree, tree.root, count);
		if (count.get() != tree.size) {
			printTree(tree, "", tree.root);
			throw new IllegalStateException("Broken tree! visited= " + count.get() +
					" size=" + tree.size);
		}
	}

	void printTree(RedBlackTree tree, String indent, int node
			) throws IOException {
		if (node == RedBlackTree.NULL) {
			System.err.println(indent + "NULL");
		} else {
			System.err.println(indent + "Node " + node + " color " +
					(tree.isRed(node) ? "red" : "black"));
			printTree(tree, indent + "  ", tree.getLeft(node));
			printTree(tree, indent + "  ", tree.getRight(node));
		}
	}

	private class MyVisitor implements StringRedBlackTree.Visitor {
		private final String[] words;
		private final int[] order;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
		int current = 0;

		MyVisitor(String[] args, int[] order) {
			words = args;
			this.order = order;
		}
		public void visit(StringRedBlackTree.VisitorContext context
				) throws IOException {
			String word = context.getText().toString();
			int  tmp=context.getOriginalPosition();
			hashMap.put(tmp,  word);
			context.writeBytes(stringOutput);
			lengthOutput.write(context.getLength());

			//System.out.println("context.getLength()    "+context.getLength());
			dumpOrder[context.getOriginalPosition()] = currentId++;
			current += 1;
		}
	}

	void checkContents(StringRedBlackTree tree, int[] order,
			String... params
			) throws IOException {
		tree.visit(new MyVisitor(params, order));
	}
	void checkContents(StringRedBlackTree tree) throws IOException {
		tree.visit(new MyVisitor(null, null));
	}

	StringRedBlackTree buildTree(String... params) throws IOException {
		StringRedBlackTree result = new StringRedBlackTree(1000);
		for(String word: params) {
			result.add(word);
			checkTree(result);
		}
		return result;
	}
public void add(String str) throws IOException{
  checkTree(dictionary);
  
  rows.add(dictionary.add(str));
}
//	@Test
	public void test1() throws Exception {
	//	StringRedBlackTree tree = new StringRedBlackTree(5);
	//	assertEquals(0, tree.getSizeInBytes());
		checkTree(dictionary);
	//	arrayList.add(tree.add("owen"));
		rows.add(dictionary.add("owen"));
		//   assertEquals(0, tree.add("owen"));
		checkTree(dictionary);
		//  assertEquals(1, tree.add("ashutosh"));
	//	arrayList.add(tree.add("ashutosh"));
		rows.add(dictionary.add("ashutosh"));
		checkTree(dictionary);
		//  assertEquals(0, tree.add("owen"));
		//arrayList.add( tree.add("owen"));
		rows.add(dictionary.add("owen"));
		checkTree(dictionary);
		// assertEquals(2, tree.add("alan"));
	//	arrayList.add(tree.add("alan"));
		rows.add(dictionary.add("alan"));
		checkTree(dictionary);
		//  assertEquals(2, tree.add("alan"));
	//	arrayList.add( tree.add("alan"));
		rows.add(dictionary.add("alan"));
		checkTree(dictionary);
		//   assertEquals(1, tree.add("ashutosh"));
	//	arrayList.add(tree.add("ashutosh"));
		rows.add(dictionary.add("ashutosh"));
		checkTree(dictionary);
	//	arrayList.add(tree.add("greg"));  
		rows.add(dictionary.add("greg"));
		// assertEquals(3, tree.add("greg"));
		checkTree(dictionary);
	//	arrayList.add(tree.add("eric"));  
		rows.add(dictionary.add("eric"));
		//   assertEquals(4, tree.add("eric"));
		checkTree(dictionary);
		//arrayList.add(tree.add("arun"));
		rows.add(dictionary.add("arun"));
		//  assertEquals(5, tree.add("arun"));
		checkTree(dictionary);

		// assertEquals(6, tree.size());
		//  checkTree(tree);
	//	//arrayList.add(tree.add("eric14"));
		rows.add(dictionary.add("eric14"));
		//   assertEquals(6, tree.add("eric14"));
		checkTree(dictionary);
	//	arrayList.add(tree.add("o"));
		rows.add(dictionary.add("o"));
		//  assertEquals(7, tree.add("o"));
		checkTree(dictionary);
		//arrayList.add(tree.add("ziggy"));
		rows.add(dictionary.add("ziggy"));
		// assertEquals(8, tree.add("ziggy"));
		checkTree(dictionary);
//		arrayList.add(tree.add("z"));
		rows.add(dictionary.add("z"));
		// assertEquals(9, tree.add("z"));
		checkTree(dictionary);

	///	tree.clear();
		//checkTree(tree);
//		assertEquals(0, tree.getSizeInBytes());
//		assertEquals(0, tree.getCharacterSize());
	}
	private void  iterator() throws IOException{
		checkContents(dictionary);
	}
	public OutStream createStream(int column,
			OrcProto.Stream.Kind kind
			) throws IOException {
		StreamName name = new StreamName(column, kind);
		BufferedStream result = null ;//BufferedStream
		if (result == null) {
			//		result = new BufferedStream(name.toString(), bufferSize, null);    OutStream TestInStream  TestInStream
			result = new BufferedStream(name.toString(),INITIAL_DICTIONARY_SIZE, null);	

			//streams.put(name, result);
		}
		return result.outStream;
	}
	IntegerWriter createIntegerWriter(PositionedOutputStream output,
			boolean signed, boolean isDirectV2) {
		if (isDirectV2) {
			return new RunLengthIntegerWriterV2(output, signed);
		} else {
			return new RunLengthIntegerWriter(output, signed);
		}
	}
	public void  init() throws IOException{



		stringOutput=new OutStream("test1", 1000, null, collect1) ;
		lengthOutput=new RunLengthIntegerWriterV2(
				new OutStream("test2", 1000, null, collect2), false);
		rowOutput =new RunLengthIntegerWriterV2(
				new OutStream("test3", 1000, null, collect3), false);
		//		stringOutput = createStream(0,
		//				OrcProto.Stream.Kind.DICTIONARY_DATA);
		//		
		//		lengthOutput = createIntegerWriter(createStream(1,
		//				OrcProto.Stream.Kind.LENGTH), false, isDirectV2);
		//		rowOutput = createIntegerWriter(createStream(2,
		//				OrcProto.Stream.Kind.DATA), false, isDirectV2);

	}

	public void flush() throws IOException{
		System.out.println("293    "+stringOutput.getBufferSize()); ;
		//BufferedStream bfs=	(BufferedStream) stringOutput.receiver;

		//	System.out.println("293    "+bfs.getBufferSize()); ;
		stringOutput.flush();
		//	System.out.println("293    "+stringOutput.getBufferSize()); ;
		lengthOutput.flush();
		rowOutput.flush();

		//directStreamOutput.flush();
		//directLengthOutput.flush();
		// reset all of the fields to be ready for the next stripe.
		//		dictionary.clear();
		//		rows.clear();
		//		stringOutput.clear();

	}
	private void rowoutPut() throws IOException{
//		System.out.println("287   dumpOrder   "+ dumpOrder.length);
//		System.out.println("287 rows.size()   "+rows.size());
		for(int i=0;i<rows.size();i++){
			rowOutput.write(dumpOrder[rows.get(i)]);
		}
	}
	private void readerInit() throws IOException{
		// read the dictionary blob
		// int dictionarySize = encodings.get(columnId).getDictionarySize();
		//int dictionarySize = dictionarySize ;
		StreamName name = new StreamName(0,
				OrcProto.Stream.Kind.DICTIONARY_DATA);
		//System.out.println("1003 name =   "+name);
		//	InStream in = streams.get(name);
		ByteBuffer inBuf1 = ByteBuffer.allocate(collect1.buffer.size());
		collect1.buffer.setByteBuffer(inBuf1, 0, collect1.buffer.size());
		inBuf1.flip();
		InStream in = InStream.create
				("test1", inBuf1, null, dictionarySize) ;
		if (in.available() > 0) {
			dictionaryBuffer = new DynamicByteArray(64, in.available());
			dictionaryBuffer.readAll(in);
			System.out.println("1006   dictionaryBuffer  "+dictionaryBuffer.size());

			//		} else {
			//			dictionaryBuffer = null;
			//		}
			in.close();

			// read the lengths    google  proto buffer
			name = new StreamName(1, OrcProto.Stream.Kind.LENGTH);
			//	in = streams.get(name);
			ByteBuffer inBuf2 = ByteBuffer.allocate(collect2.buffer.size());
			collect2.buffer.setByteBuffer(inBuf2, 0, collect2.buffer.size());
			inBuf2.flip();
			in = InStream.create
					("test2", inBuf2, null, dictionarySize) ;
			System.out.println("359   "+inBuf2.arrayOffset());
			//		IntegerReader lenReader = createIntegerReader(encodings.get(columnId)
			//				.getKind(), in, false);
			IntegerReader lenReader = createIntegerReader(OrcProto.ColumnEncoding.Kind.DIRECT_V2, in, false);
			int offset = 0;
			//		if (dictionaryOffsets == null ||
			//				dictionaryOffsets.length < dictionarySize + 1) {
			dictionaryOffsets = new int[dictionarySize + 1];
			//	}



			//			for(int j=0;j<10;j++){
			//				System.out.println("374//////////////////////////////////   "+(int) lenReader.next());	
			//			}
			for(int i=0; i < dictionarySize; ++i) {


				dictionaryOffsets[i] = offset;
//				if(i<15){
//					System.out.println("1028  offset  "+offset+"   dictionaryOffsets[i]=     "+ dictionaryOffsets[i]);
//				}
				offset += (int) lenReader.next();
			}
			dictionaryOffsets[dictionarySize] = offset;
			in.close();
			// set up the row reader
			name = new StreamName(2, OrcProto.Stream.Kind.DATA);
			ByteBuffer inBuf3 = ByteBuffer.allocate(collect3.buffer.size());
			collect3.buffer.setByteBuffer(inBuf3, 0, collect3.buffer.size());
			inBuf3.flip();
			in = InStream.create
					("test3", inBuf3, null, dictionarySize) ;
			reader = createIntegerReader(OrcProto.ColumnEncoding.Kind.DIRECT_V2,
					in, false);
		}
	}

	private  String  readEachValue(Text previous) throws IOException{
		Text result = null;
		// if (valuePresent) {
		/////////////////////////////////////////////// ////this entry is used  for replace idNumber 0 1 2 3 4 5
		int entry = (int) reader.next();
		if (previous == null) {
			result = new Text();
		} else {
			result = (Text) previous;
		}
		int offset = dictionaryOffsets[entry];//////////////////////////////////////////this offset replace length of String length
		int length;
		// if it isn't the last entry, subtract the offsets otherwise use
		// the buffer length.
		if (entry < dictionaryOffsets.length - 1) {
			length = dictionaryOffsets[entry + 1] - offset;
		} else {
			length = dictionaryBuffer.size() - offset;
		}
		// If the column is just empty strings, the size will be zero,
		// so the buffer will be null, in that case just return result
		// as it will default to empty
		if (dictionaryBuffer != null) {
			dictionaryBuffer.setText(result, offset, length);
		} else {
			result.clear();
		}
		// }
		return result.toString();

	}
	IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
			InStream in,
			boolean signed) throws IOException {
		switch (kind) {
		case DIRECT_V2:
		case DICTIONARY_V2:
			return new RunLengthIntegerReaderV2(in, signed);
		case DIRECT:
		case DICTIONARY:
			return new RunLengthIntegerReader(in, signed);
		default:
			throw new IllegalArgumentException("Unknown encoding " + kind);
		}
	}
	private void foreach() throws IOException{
	//	System.out.println("434    rows.size()  "+rows.size());
		for(int i=0;i<rows.size();i++){
			System.out.println("result  "+readEachValue(null));
		}
	}
	public static void main(String[] args) throws Exception {
		TestStringRedBlackTree test = new TestStringRedBlackTree();
		test.test1();
		test.dumpOrder = new int[test.dictionary.size()];
		test.dictionarySize=test.dictionary.size();
		test.init();
		test.iterator();
		test.rowoutPut();
		test.flush();
//		System.out.println("446   "+collect1.buffer.size());
//		System.out.println("446   "+collect2.buffer.size());
//		System.out.println("446   "+collect3.buffer.size());


		test.readerInit();
		test.foreach();
		/////////////////////////////finish Write


	}
}
