/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.ac.ncic.mastiff.io.coding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;


//import org.apache.hadoop.hive.mastiff.StreamName;
//import org.apache.hadoop.hive.mastiff.ORCStringecnodingUtil.MyVisitor;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import FlexibleEncoding.ORC.BufferedStream;
import FlexibleEncoding.ORC.DynamicByteArray;
import FlexibleEncoding.ORC.DynamicIntArray;
import FlexibleEncoding.ORC.InStream;
import FlexibleEncoding.ORC.IntegerReader;
import FlexibleEncoding.ORC.IntegerWriter;
import FlexibleEncoding.ORC.OrcProto;
import FlexibleEncoding.ORC.OutStream;
import FlexibleEncoding.ORC.PositionedOutputStream;
import FlexibleEncoding.ORC.RedBlackTree;
import FlexibleEncoding.ORC.RunLengthIntegerReader;
import FlexibleEncoding.ORC.RunLengthIntegerReaderV2;
import FlexibleEncoding.ORC.RunLengthIntegerWriter;
import FlexibleEncoding.ORC.RunLengthIntegerWriterV2;
import FlexibleEncoding.ORC.StreamName;
import FlexibleEncoding.ORC.StringRedBlackTree;
import FlexibleEncoding.ORC.TestInStream;
import FlexibleEncoding.ORC.TestStringRedBlackTree;

public class ORCStringEcnodingUtil {
  private static  HashMap<Integer,String>   hashMap=new HashMap<Integer,String>() ;
  private final ArrayList<Integer>  arrayList=new ArrayList<Integer>() ;
  private static final int INITIAL_DICTIONARY_SIZE = 4096;
  public   OutStream stringOutput;
  public   IntegerWriter lengthOutput;
  public IntegerWriter rowOutput;
  public   StringRedBlackTree dictionary  =new StringRedBlackTree(INITIAL_DICTIONARY_SIZE);
  private final boolean isDirectV2 = true;
  public   DynamicIntArray rows = new DynamicIntArray();
  public    int[] dumpOrder ;
  private  int currentId=0;
  public  int dictionarySize=0;
  public DynamicByteArray dictionaryBuffer;
  public  int[] dictionaryOffsets;
  private IntegerReader reader;
  private final StringRedBlackTree tree = new StringRedBlackTree(5);
  public  final TestInStream.OutputCollector collect1 = new TestInStream.OutputCollector();
  public   TestInStream.OutputCollector collect2 = new TestInStream.OutputCollector();
  public     TestInStream.OutputCollector collect3 = new TestInStream.OutputCollector();
  /**
   * Checks the validity of the entire tree. Also ensures that the number of
   * nodes visited is the same as the size of the set.
   */
  public void checkTree(StringRedBlackTree tree) throws IOException {
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
  public   int checkSubtree(RedBlackTree tree, int node, IntWritable count
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
  private  class MyVisitor implements StringRedBlackTree.Visitor {
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
      context.writeBytes(stringOutput);
      lengthOutput.write(context.getLength());
      dumpOrder[context.getOriginalPosition()] = currentId++;
      current += 1;
    }
  }
  public  void  iterator() throws IOException{
    checkContents(dictionary);
  }
  public  OutStream createStream(int column,
      OrcProto.Stream.Kind kind
      ) throws IOException {
    FlexibleEncoding.ORC.StreamName name = new FlexibleEncoding.ORC.StreamName(column, kind);
    BufferedStream result = null ;
    if (result == null) {
      result = new BufferedStream(name.toString(),INITIAL_DICTIONARY_SIZE, null);
    }
    return result.outStream;
  }
public   IntegerWriter createIntegerWriter(PositionedOutputStream output,
      boolean signed, boolean isDirectV2) {
    if (isDirectV2) {
      return new RunLengthIntegerWriterV2(output, signed);
    } else {
      return new RunLengthIntegerWriter(output, signed);
    }
  }
  public void  add(String str) throws IOException{
    checkTree(dictionary);
    rows.add(dictionary.add(str));
  }

  public void  init() throws IOException{

    stringOutput=new  OutStream("test1", 1000, null, collect1) ;
    lengthOutput=new RunLengthIntegerWriterV2(
        new OutStream("test2", 1000, null, collect2), false);
    rowOutput =new RunLengthIntegerWriterV2(
        new OutStream("test3", 1000, null, collect3), false);
    //    stringOutput = createStream(0,
    //        OrcProto.Stream.Kind.DICTIONARY_DATA);
    //
    //    lengthOutput = createIntegerWriter(createStream(1,
    //        OrcProto.Stream.Kind.LENGTH), false, isDirectV2);
    //    rowOutput = createIntegerWriter(createStream(2,
    //        OrcProto.Stream.Kind.DATA), false, isDirectV2);

  }

  public void flush() throws IOException{
    System.out.println("293    "+stringOutput.getBufferSize()); ;
    //BufferedStream bfs= (BufferedStream) stringOutput.receiver;
    stringOutput.flush();
    lengthOutput.flush();
    rowOutput.flush();
    //directStreamOutput.flush();
    //directLengthOutput.flush();
    // reset all of the fields to be ready for the next stripe.
    //    dictionary.clear();
    //    rows.clear();
    //    stringOutput.clear();

  }
   public void rowoutPut() throws IOException{
    for(int i=0;i<rows.size();i++){
      rowOutput.write(dumpOrder[rows.get(i)]);
    }
  }
  public void readerInit() throws IOException{

    FlexibleEncoding.ORC.StreamName name = new  FlexibleEncoding.ORC.StreamName(0,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    //  InStream in = streams.get(name);
    ByteBuffer inBuf1 = ByteBuffer.allocate(collect1.buffer.size());
    collect1.buffer.setByteBuffer(inBuf1, 0, collect1.buffer.size());
    inBuf1.flip();
    InStream in = InStream.create
        ("test1", inBuf1, null, dictionarySize) ;
    if (in.available() > 0) {
      dictionaryBuffer = new DynamicByteArray(64, in.available());
      dictionaryBuffer.readAll(in);
      in.close();
      // read the lengths    google  proto buffer
      name = new StreamName(1, OrcProto.Stream.Kind.LENGTH);
      //  in = streams.get(name);
      ByteBuffer inBuf2 = ByteBuffer.allocate(collect2.buffer.size());
      collect2.buffer.setByteBuffer(inBuf2, 0, collect2.buffer.size());
      inBuf2.flip();
      in = InStream.create
          ("test2", inBuf2, null, dictionarySize) ;
      //    IntegerReader lenReader = createIntegerReader(encodings.get(columnId)
      //        .getKind(), in, false);
      IntegerReader lenReader = createIntegerReader(OrcProto.ColumnEncoding.Kind.DIRECT_V2, in, false);
      int offset = 0;
      dictionaryOffsets = new int[dictionarySize + 1];
      for(int i=0; i < dictionarySize; ++i) {
        dictionaryOffsets[i] = offset;
        offset += (int) lenReader.next();
      }
      dictionaryOffsets[dictionarySize] = offset;
      in.close();
      name = new FlexibleEncoding.ORC.StreamName(2, OrcProto.Stream.Kind.DATA);
      ByteBuffer inBuf3 = ByteBuffer.allocate(collect3.buffer.size());
      collect3.buffer.setByteBuffer(inBuf3, 0, collect3.buffer.size());
      inBuf3.flip();
      in = InStream.create
          ("test3", inBuf3, null, dictionarySize) ;
      reader = createIntegerReader(OrcProto.ColumnEncoding.Kind.DIRECT_V2,
          in, false);
    }
  }

  public  String  readEachValue(Text previous) throws IOException{
    Text result = null;
    int entry = (int) reader.next();
    if (previous == null) {
      result = new Text();
    } else {
      result = (Text) previous;
    }
    int offset = dictionaryOffsets[entry];
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
    return result.toString();
  }
 public  IntegerReader createIntegerReader(OrcProto.ColumnEncoding.Kind kind,
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
 public void foreach() throws IOException{
    for(int i=0;i<rows.size();i++){
      System.out.println("result  "+readEachValue(null));
    }
  }
  public static void main(String[] args) throws Exception {
    ORCStringEcnodingUtil test = new  ORCStringEcnodingUtil();
    //  test.test1();
    //  test.dumpOrder = new int[test.dictionary.size()];
    //  test.dictionarySize=dictionary.size();
    test.init();
    test.iterator();
    test.rowoutPut();
    test.flush();
    test.readerInit();
    test.foreach();


  }
}
