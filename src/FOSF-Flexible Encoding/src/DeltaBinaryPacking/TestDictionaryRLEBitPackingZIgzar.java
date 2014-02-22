package DeltaBinaryPacking;

/*
 * this is used for test  various encoding  from Parquet and ORC file . also used to  test some new  combinations encodings  of basic encoding ways from Parquet and ORC ,which is combineed  by  wangmeng
 * @author  wangmeng
 */

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
public class TestDictionaryRLEBitPackingZIgzar {
	//@Test
	  public static void testIntDictionary() throws IOException {

	    int COUNT = 2000;
	   // int COUNT2 = 4000;
	    final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(10000, 10000);
    //   FileOutputStream fos =FileOutputStream(new File("/home/wangmeng/sourceDictionary"));
       FileOutputStream  fos =new FileOutputStream(new File("/home/wangmeng/sourceDictionary"));
       DataOutputStream dos=new DataOutputStream(fos);
	    for (int i = 0; i < COUNT; i++) {
	      dos.writeInt(i % 50);
	    	cw.writeInteger(i % 50);
	    	
	    }
	    dos.close();
	   BytesInput bytes1 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
	 //   assertEquals(50, cw.getDictionarySize());

//	    for (int i = COUNT2; i > 0; i--) {
//	      cw.writeInteger(i % 50);
//	    }
	   // BytesInput bytes2 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
	  //  assertEquals(50, cw.getDictionarySize());

	    DictionaryValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.INT32);
//need to provide   count byte[]  and offset
	   cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
	    for (int i = 0; i < COUNT; i++) {
	      int back = cr.readInteger();
	   //   assertEquals(i % 50, back);
	    }

//	    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
//	    for (int i = COUNT2; i > 0; i--) {
//	      int back = cr.readInteger();
//	   //   assertEquals(i % 50, back);
//	    }

	  }
	  private static BytesInput getBytesAndCheckEncoding(ValuesWriter cw, Encoding encoding)
		      throws IOException {
		  
		  BytesInput  bit=cw.getBytes();
		  System.out.println("67  "+bit.size());  
		 byte[] byt= bit.toByteArray();////inner  write data to disk  wm
		 
System.out.println("69  "+byt.length);



//		 BytesInput bytes = BytesInput.copy(bit);
//		 System.out.println("72  "+bytes.size()); 
	//	    assertEquals(encoding, cw.getEncoding());
	         cw.reset();
		    return bit;
		  }
	  
	  //come from  readPage  dictionary wm
//	  this.path = checkNotNull(path, "path");
//	    this.pageReader = checkNotNull(pageReader, "pageReader");
//	    this.converter = checkNotNull(converter, "converter");
//	    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
//	    if (dictionaryPage != null) {
//	      try {
//	        this.dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
//	        if (converter.hasDictionarySupport()) {
//	          converter.setDictionary(dictionary);
//	        }
//	      } catch (IOException e) {
//	        throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
//	      }
//	    } else {
//	      this.dictionary = null;
//	    }
//	    this.totalValueCount = pageReader.getTotalValueCount();  
	  private static DictionaryValuesReader initDicReader(ValuesWriter cw, PrimitiveType.PrimitiveTypeName type)
		      throws IOException {
		  //need  know  dictionary size(50)  dictionaryBytesSize(50*4)  dictionary BytsInput  wm
		    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
		    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, type, 0, 0);
		    final Dictionary dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//		PageReader pageReader=  pageReadStore.getPageReader(path);  ColumnDescriptor path
//		  
//		  DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
//		  
//		  dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
//	//	   this.dataColumn = page.getValueEncoding().getDictionaryBasedValuesReader(path, ValuesType.VALUES, dictionary);
		  
		  
		    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);
		
		    
		    
		    return cr;
		  }
	  private void roundTripInt(DictionaryValuesWriter cw,  ValuesReader reader, int maxDictionaryByteSize) throws IOException {
	    int fallBackThreshold = maxDictionaryByteSize / 4;
	    for (int i = 0; i < 100; i++) {
	      cw.writeInteger(i);
	      if (i < fallBackThreshold) {
	    //    assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
	      } else {
	  //      assertEquals(cw.getEncoding(), PLAIN);
	      }
	    }

	    reader.initFromPage(100, cw.getBytes().toByteArray(), 0);

	    for (int i = 0; i < 100; i++) {
	 //     assertEquals(i, reader.readInteger());
	    }
	  }
	  
//	  @Test
	  public void testIntDictionaryFallBack() throws IOException {
	    int slabSize = 100;
	    int maxDictionaryByteSize = 50;
	    final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
	    
	    // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
	    ValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
	    
	    roundTripInt(cw, reader, maxDictionaryByteSize);
	    //simulate cutting the page
	    cw.reset();
	//    assertEquals(0,cw.getBufferedSize());
	    cw.resetDictionary();
	  
	    roundTripInt(cw, reader, maxDictionaryByteSize);
	  }
	  
	//  @Test
	  public static void testBinaryDictionary() throws IOException {
	    int COUNT = 100;
	    ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(200, 10000);///200  stand for max dictionary bytes size  10000  stand for  the inital buffer size to store value 
//atach to  the  Max Size  and the number of dictionary  id  can not > Integer.Max  wm
	    writeRepeated(COUNT, cw, "b");
	    System.out.println("191"+cw.getBufferedSize());
	    BytesInput bytes2 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
	    System.out.println("193"+bytes2.size());
//	    
//	    writeRepeated(COUNT, cw, "a");
//	   System.out.println("187"+cw.getBufferedSize());
//	    BytesInput bytes1 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
//	    System.out.println("189"+bytes1.size());
	    // now we will fall back
//	    writeDistinct(COUNT, cw, "c");
//	    BytesInput bytes3 = getBytesAndCheckEncoding(cw, Encoding.PLAIN);
//	    System.out.println("199   "+bytes3.size());
	    DictionaryValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.BINARY);
	//    checkRepeated(COUNT, bytes1, cr, "a");
	    checkRepeated(COUNT, bytes2, cr, "b");
//	    BinaryPlainValuesReader cr2 = new BinaryPlainValuesReader();
//	    checkDistinct(COUNT, bytes3, cr2, "c");
	  }

	//  @Test
	  public void testBinaryDictionaryFallBack() throws IOException {
	    int slabSize = 100;
	    int maxDictionaryByteSize = 50;
	    final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
	    int fallBackThreshold = maxDictionaryByteSize;
	    int dataSize=0;
	    for (long i = 0; i < 100; i++) {
	      Binary binary = Binary.fromString("str" + i);
	      cw.writeBytes(binary);
	      dataSize+=(binary.length()+4);
	      if (dataSize < fallBackThreshold) {
	//        assertEquals( PLAIN_DICTIONARY,cw.getEncoding());
	      } else {
	//        assertEquals(PLAIN,cw.getEncoding());
	      }
	    }

	    //Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
	    ValuesReader reader = new BinaryPlainValuesReader();
	    reader.initFromPage(100, cw.getBytes().toByteArray(), 0);

	    for (long i = 0; i < 100; i++) {
	//      assertEquals(Binary.fromString("str" + i), reader.readBytes());
	    }

	    //simulate cutting the page
	    cw.reset();
	//    assertEquals(0,cw.getBufferedSize());
	  }

	//  @Test
	  public void testFirstPageFallBack() throws IOException {
	    int COUNT = 1000;
	    ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(10000, 10000);
	    writeDistinct(COUNT, cw, "a");
	    // not efficient so falls back
	    BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
	    writeRepeated(COUNT, cw, "b");
	    // still plain because we fell back on first page
	    BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);

	    ValuesReader cr = new BinaryPlainValuesReader();
	    checkDistinct(COUNT, bytes1, cr, "a");
	    checkRepeated(COUNT, bytes2, cr, "b");

	  }

	  //@Test
	  public void testSecondPageFallBack() throws IOException {

	    int COUNT = 1000;
	    ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(1000, 10000);
	    writeRepeated(COUNT, cw, "a");
	    BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
	    writeDistinct(COUNT, cw, "b");
	    // not efficient so falls back
	    BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
	    writeRepeated(COUNT, cw, "a");
	    // still plain because we fell back on previous page
	    BytesInput bytes3 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);

	    ValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.BINARY);
	    checkRepeated(COUNT, bytes1, cr, "a");
	    cr = new BinaryPlainValuesReader();
	    checkDistinct(COUNT, bytes2, cr, "b");
	    checkRepeated(COUNT, bytes3, cr, "a");
	  }

	 // @Test
	  public static void testLongDictionary() throws IOException {

	    int COUNT = 1000;
	    int COUNT2 = 2000;
	    final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(10000, 10000);
	    for (long i = 0; i < COUNT; i++) {
	      cw.writeLong(i % 50);
	    }
	    BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
	  //  assertEquals(50, cw.getDictionarySize());
//
//	    for (long i = COUNT2; i > 0; i--) {
//	      cw.writeLong(i % 50);
//	    }
//	    BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
	//    assertEquals(50, cw.getDictionarySize());

	    DictionaryValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.INT64);

	    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
	    for (long i = 0; i < COUNT; i++) {
	      long back = cr.readLong();
	  //    assertEquals(i % 50, back);
	    }

//	//    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
//	    for (long i = COUNT2; i > 0; i--) {
//	      long back = cr.readLong();
//	 //     assertEquals(i % 50, back);
//	    }
	    }
	  
	  private void roundTripLong(DictionaryValuesWriter cw,  ValuesReader reader, int maxDictionaryByteSize) throws IOException {
	    int fallBackThreshold = maxDictionaryByteSize / 8;
	    for (long i = 0; i < 100; i++) {
	      cw.writeLong(i);
	      if (i < fallBackThreshold) {
	//        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
	      } else {
	//        assertEquals(cw.getEncoding(), PLAIN);
	      }
	    }

	    reader.initFromPage(100, cw.getBytes().toByteArray(), 0);

	    for (long i = 0; i < 100; i++) {
	///      assertEquals(i, reader.readLong());
	    }
	  }

	 // @Test
	  public void testLongDictionaryFallBack() throws IOException {
	    int slabSize = 100;
	    int maxDictionaryByteSize = 50;
	    final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
	    // Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
	    ValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
	    
	    roundTripLong(cw, reader, maxDictionaryByteSize);
	    //simulate cutting the page
	    cw.reset();
	 //   assertEquals(0,cw.getBufferedSize());
	    cw.resetDictionary();
	  
	    roundTripLong(cw, reader, maxDictionaryByteSize);
	  }

	  
	  private static void checkDistinct(int COUNT, BytesInput bytes, ValuesReader cr, String prefix) throws IOException {
		    cr.initFromPage(COUNT, bytes.toByteArray(), 0);
		    for (int i = 0; i < COUNT; i++) {
	//	      Assert.assertEquals(prefix + i, cr.readBytes().toStringUsingUTF8());
		    }
		  }

		  private static void checkRepeated(int COUNT, BytesInput bytes, ValuesReader cr, String prefix) throws IOException {
		    cr.initFromPage(COUNT, bytes.toByteArray(), 0);
		    for (int i = 0; i < COUNT; i++) {
		//      Assert.assertEquals(prefix + i % 10, cr.readBytes().toStringUsingUTF8());
		    }
		  }

		  private static void writeDistinct(int COUNT, ValuesWriter cw, String prefix) {
		    for (int i = 0; i < COUNT; i++) {
		      cw.writeBytes(Binary.fromString(prefix + i));
		    }
		  }

		  private static void writeRepeated(int COUNT, ValuesWriter cw, String prefix) {
		    for (int i = 0; i < COUNT; i++) {
		      cw.writeBytes(Binary.fromString(prefix + i % 10));
		    }
		  }

	  
	  public static void main(String[] args) throws IOException {
			// TODO Auto-generated method stub
		   testIntDictionary();
		  testBinaryDictionary();
		  testLongDictionary();
		}

	  
}
