package FlexibleEncoding.Parquet;

/*
 * this is used for test  various encoding  from Parquet and ORC file . also used to  test some new  combinations encodings  of basic encoding ways from Parquet and ORC ,which is combineed  by  wangmeng
 * @author  wangmeng
 */
import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestDictionaryRLEBitPackingZIgzar {
	static java.util.Calendar c=java.util.Calendar.getInstance();    
	static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒"); 
	public static  int fileLong ;
	public static void testIntDictionary(String[] s) throws IOException {
		System.out.println("intDictionaryRLEBitPackingZIgzar :  begin write to DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length()/4;
		System.out.println("fileLong  :  "+fileLong);
		final DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter cw = new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(Integer.MAX_VALUE, fileLong);
		for (int i = 0; i < fileLong; i++) {
			cw.writeInteger(dis.readInt());
		}

		dis.close();
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}
		
		cw.getBytes().writeToDisk(str);
		cw.WriteDictionaryToDisk(s);
		

		System.out.println("intDictionaryRLEBitPackingZIgzar :  finish  write to DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 


		System.out.println("intDictionaryRLEBitPackingZIgzar:  begin read  from  DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		byte[] bytes=getBytes(s);
		System.out.println(bytes.length);
		DictionaryValuesReader cr = initDicReader(s,PrimitiveType.PrimitiveTypeName.INT32);
		cr.initFromPage(fileLong, bytes, 0);
		int count=0 ;
		for (int i = 0; i < fileLong; i++) {
			
			int back = cr.readInteger();
			count ++ ;
		//	System.out.println("back =:  "+back);

		}
		System.out.println("totoal number =:  "+count);

		System.out.println("intDictionaryRLEBitPackingZIgzar : finish read  from  DictionaryRLE  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
	}


	private static  byte[] getBytes(String[] s) throws IOException{

		//	  BytesInput  bit=cw.getBytes();
		//		  System.out.println("67  "+bit.size());  
		//	 byte[] byt= bit.toByteArray();////inner  write data to disk  wm

		File file=new File(s[1]);
		DataInputStream dis =new DataInputStream(new FileInputStream(file));
		byte[] byt=new byte[(int) file.length()];
		//	  System.out.println(dis.readInt());
		//	  System.out.println(dis.readInt());
		dis.readFully(byt);

		return byt;
	}

	private static DictionaryValuesReader initDicReader(String[] s , PrimitiveType.PrimitiveTypeName type)
			throws IOException {
		//need  know  dictionary size(50)  dictionaryBytesSize(50*4)  dictionary BytsInput  wm
		//  final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();

		//  new DictionaryPage(dictionaryEncoder.getBytes(), lastUsedDictionarySize, Encoding.PLAIN_DICTIONARY);
		//		  final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy() ;
		File file =new File(s[3]);
		DataInputStream dis =new DataInputStream(new FileInputStream(file));
		byte[] bytes=new byte[(int) (file.length()-4)];
		//System.out.println(file.length()-4);

		int 	DictionarySize = dis.readInt();
		dis.readFully(bytes);
		dis.close();
		CapacityByteArrayOutputStream  cbs=new CapacityByteArrayOutputStream(bytes.length);
		cbs.write(bytes, 0, bytes.length);
		BytesInput  bytesInput =new   BytesInput.CapacityBAOSBytesInput(cbs) ;

		//  DictionaryPage dictionaryPage =new  DictionaryPage(200,50,Encoding.PLAIN_DICTIONARY);
		DictionaryPage dictionaryPage =new  DictionaryPage( bytesInput,DictionarySize,Encoding.PLAIN_DICTIONARY);
		//	System.out.println(bytesInput.toByteArray().length);
		final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, type, 0, 0);
		Encoding    encoding=Encoding.PLAIN_DICTIONARY ;
		final Dictionary dictionary =  encoding.initDictionary(descriptor, dictionaryPage);		 

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

//	//  @Test
//	public static void testBinaryDictionary() throws IOException {
//		int COUNT = 100;
//		ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(200, 10000);///200  stand for max dictionary bytes size  10000  stand for  the inital buffer size to store value 
//		//atach to  the  Max Size  and the number of dictionary  id  can not > Integer.Max  wm
//		writeRepeated(COUNT, cw, "b");
//		System.out.println("191"+cw.getBufferedSize());
//		BytesInput bytes2 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
//		System.out.println("193"+bytes2.size());
//		//	    
//		//	    writeRepeated(COUNT, cw, "a");
//		//	   System.out.println("187"+cw.getBufferedSize());
//		//	    BytesInput bytes1 = getBytesAndCheckEncoding(cw, Encoding.PLAIN_DICTIONARY);
//		//	    System.out.println("189"+bytes1.size());
//		// now we will fall back
//		//	    writeDistinct(COUNT, cw, "c");
//		//	    BytesInput bytes3 = getBytesAndCheckEncoding(cw, Encoding.PLAIN);
//		//	    System.out.println("199   "+bytes3.size());
//		DictionaryValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.BINARY);
//		//    checkRepeated(COUNT, bytes1, cr, "a");
//		checkRepeated(COUNT, bytes2, cr, "b");
//		//	    BinaryPlainValuesReader cr2 = new BinaryPlainValuesReader();
//		//	    checkDistinct(COUNT, bytes3, cr2, "c");
//	}

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
//
//	//  @Test
//	public void testFirstPageFallBack() throws IOException {
//		int COUNT = 1000;
//		ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(10000, 10000);
//		writeDistinct(COUNT, cw, "a");
//		// not efficient so falls back
//		BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
//		writeRepeated(COUNT, cw, "b");
//		// still plain because we fell back on first page
//		BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
//
//		ValuesReader cr = new BinaryPlainValuesReader();
//		checkDistinct(COUNT, bytes1, cr, "a");
//		checkRepeated(COUNT, bytes2, cr, "b");
//
//	}

//	//@Test
//	public void testSecondPageFallBack() throws IOException {
//
//		int COUNT = 1000;
//		ValuesWriter cw = new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(1000, 10000);
//		writeRepeated(COUNT, cw, "a");
//		BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
//		writeDistinct(COUNT, cw, "b");
//		// not efficient so falls back
//		BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
//		writeRepeated(COUNT, cw, "a");
//		// still plain because we fell back on previous page
//		BytesInput bytes3 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN);
//
//		ValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.BINARY);
//		checkRepeated(COUNT, bytes1, cr, "a");
//		cr = new BinaryPlainValuesReader();
//		checkDistinct(COUNT, bytes2, cr, "b");
//		checkRepeated(COUNT, bytes3, cr, "a");
//	}
//
//	// @Test
//	public static void testLongDictionary() throws IOException {
//
//		int COUNT = 1000;
//		int COUNT2 = 2000;
//		final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(10000, 10000);
//		for (long i = 0; i < COUNT; i++) {
//			cw.writeLong(i % 50);
//		}
//		BytesInput bytes1 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
//		//  assertEquals(50, cw.getDictionarySize());
//		//
//		//	    for (long i = COUNT2; i > 0; i--) {
//		//	      cw.writeLong(i % 50);
//		//	    }
//		//	    BytesInput bytes2 = getBytesAndCheckEncoding(cw,  Encoding.PLAIN_DICTIONARY);
//		//    assertEquals(50, cw.getDictionarySize());
//
//		DictionaryValuesReader cr = initDicReader(cw, PrimitiveType.PrimitiveTypeName.INT64);
//
//		cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
//		for (long i = 0; i < COUNT; i++) {
//			long back = cr.readLong();
//			//    assertEquals(i % 50, back);
//		}
//
//		//	//    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
//		//	    for (long i = COUNT2; i > 0; i--) {
//		//	      long back = cr.readLong();
//		//	 //     assertEquals(i % 50, back);
//		//	    }
//	}
//
//	private void roundTripLong(DictionaryValuesWriter cw,  ValuesReader reader, int maxDictionaryByteSize) throws IOException {
//		int fallBackThreshold = maxDictionaryByteSize / 8;
//		for (long i = 0; i < 100; i++) {
//			cw.writeLong(i);
//			if (i < fallBackThreshold) {
//				//        assertEquals(cw.getEncoding(), PLAIN_DICTIONARY);
//			} else {
//				//        assertEquals(cw.getEncoding(), PLAIN);
//			}
//		}
//
//		reader.initFromPage(100, cw.getBytes().toByteArray(), 0);
//
//		for (long i = 0; i < 100; i++) {
//			///      assertEquals(i, reader.readLong());
//		}
//	}
//
//	// @Test
//	public void testLongDictionaryFallBack() throws IOException {
//		int slabSize = 100;
//		int maxDictionaryByteSize = 50;
//		final DictionaryValuesWriter cw = new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(maxDictionaryByteSize, slabSize);
//		// Fallbacked to Plain encoding, therefore use PlainValuesReader to read it back
//		ValuesReader reader = new PlainValuesReader.LongPlainValuesReader();
//
//		roundTripLong(cw, reader, maxDictionaryByteSize);
//		//simulate cutting the page
//		cw.reset();
//		//   assertEquals(0,cw.getBufferedSize());
//		cw.resetDictionary();
//
//		roundTripLong(cw, reader, maxDictionaryByteSize);
//	}


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
		testIntDictionary(args);
		//		  testBinaryDictionary();
		//		  testLongDictionary();
	}


}
