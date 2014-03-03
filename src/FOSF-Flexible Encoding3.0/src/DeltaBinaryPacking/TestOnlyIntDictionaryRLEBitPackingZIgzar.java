package DeltaBinaryPacking;

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

import DeltaBinaryPacking.PlainValuesDictionary.PlainIntegerDictionary;
public class TestOnlyIntDictionaryRLEBitPackingZIgzar {
	static java.util.Calendar c=java.util.Calendar.getInstance();    
	static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒"); 
	public static  int fileLong =0 ,count =0 ,runcount=0 ;
	public static   long startTime ,encodingReadTime ,encodingTime,encodingWriteTime, RencodingReadTime ,RencodingTime,RencodingWriteTime, finalTime;
	public static   long DeltaencodingReadTime=0 ,DeltaencodingTime,DeltaencodingWriteTime=0, DeltaRencodingReadTime=0 ,DeltaRencodingTime=0,DeltaRencodingWriteTime=0;
	public static void testIntDictionary(String[] s) throws IOException {
		//	System.out.println("intOnlyDictionaryRLEBitPackingZIgzar :  begin write to DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		encodingReadTime=System.currentTimeMillis() ;
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length()/4;
		int[] initValues=new int[fileLong] ;

		for (int i = 0; i < fileLong; i++) {
			//cw.writeInteger(dis.readInt());
			initValues[i]=dis.readInt() ;
		}
		fis.close();
		dis.close();
		encodingTime=System.currentTimeMillis() ;
		DeltaencodingReadTime=DeltaencodingReadTime+encodingTime-encodingReadTime ;		
		final OnlyDictionaryValuesWriter.PlainIntegerDictionaryValuesWriter cw = new OnlyDictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(Integer.MAX_VALUE, fileLong);
		for (int i = 0; i < fileLong; i++) {
			cw.writeInteger(initValues[i]);
		}

		encodingWriteTime=System.currentTimeMillis() ;
		DeltaencodingTime=DeltaencodingTime+encodingWriteTime-encodingTime ;
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}

		cw.getBytes().writeToDisk(str);

		cw.WriteDictionaryToDisk(s);
		RencodingReadTime=System.currentTimeMillis() ;
		DeltaencodingWriteTime=DeltaencodingWriteTime+RencodingReadTime-encodingWriteTime;
		//System.out.println("intOnlyDictionaryRLEBitPackingZIgzar :  finish  write to DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		//	System.out.println("intDictionaryRLEBitPackingZIgzar:  begin read  from  DictionaryRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		byte[] bytes=getBytes(s);
		RencodingTime=System.currentTimeMillis() ;
		DeltaRencodingReadTime=DeltaRencodingReadTime+RencodingTime-RencodingReadTime;
		DictionaryValuesReader cr = initDicReader(s,PrimitiveType.PrimitiveTypeName.INT32);
		cr.initFromPage(fileLong, bytes, 0);
		int[] result=new int[fileLong];

		for (int i = 0; i < fileLong; i++) {
			result[i]= cr.readInteger();
			//		System.out.println(back);
			count++ ;
		}
		RencodingWriteTime=System.currentTimeMillis() ;
		DeltaRencodingTime=DeltaRencodingTime+RencodingWriteTime-RencodingTime;
		FileOutputStream    revrseencodingFis=new  FileOutputStream(new File(s[4])) ;
		DataOutputStream  revrseencodingDos=new DataOutputStream(revrseencodingFis) ;
		for(int i=0; i < fileLong; ++i) {
			//int x = (int) in.next();
			revrseencodingDos.writeInt(result[i]);
		}
		revrseencodingFis.close();
		revrseencodingDos.close();
		finalTime=System.currentTimeMillis() ;
		DeltaRencodingWriteTime=DeltaRencodingWriteTime+finalTime-RencodingWriteTime;

		//	System.out.println("total value  "+ count);
		//	System.out.println("intOnlyDictionaryRLEBitPackingZIgzar : finish read  from  DictionaryRLE  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
	}


	private static  byte[] getBytes(String[] s) throws IOException{

		//	  BytesInput  bit=cw.getBytes();
		//		  System.out.println("67  "+bit.size());  
		//	 byte[] byt= bit.toByteArray();////inner  write data to disk  wm

		File file=new File(s[1]);
		DataInputStream dis =new DataInputStream(new FileInputStream(file));
		byte[] byt=new byte[(int) file.length()];
		dis.readFully(byt);
        dis.close();
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




	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File sourcefileDirectory =new File(args[0]);
		File[]  sourcefiles=    sourcefileDirectory.listFiles() ;
		int length=sourcefiles.length;
		String[] sourcestr=new String[length];
		String[] encodinstr=new String[length];
		String[]  dictionaryStr=new String[length];
		String[]  revrseEncodingstr=new String[length];
		for (int i=0;i<sourcefiles.length;i++){
			sourcestr[i]=sourcefiles[i].getAbsolutePath() ;
			encodinstr[i]=args[1]+"/"+i  ;
			dictionaryStr[i]=args[3]+"/"+i  ;
			revrseEncodingstr[i]=args[4]+"/"+i  ;
			//	System.out.println(finalstr[i]);
		}
		String[]  runstr=new String[5] ;
		runstr[2]=args[2];
		startTime=System.currentTimeMillis() ;
		System.out.println("ORCRLE :  begin ORCRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		for (int i=0;i<length;i++){
			runstr[0]=sourcestr[i];
			runstr[1]=encodinstr[i] ;
			runstr[3]=dictionaryStr[i];
			runstr[4]=revrseEncodingstr[i] ;
			testIntDictionary(runstr);
			runcount++ ;
		}
		System.out.println("ORCRLE :  finish  ORCRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		System.out.println("totoal run :"+runcount+"  times");
		System.out.println("totoal valuecount :"+count);
		//DeltaencodingReadTime=0 ,DeltaencodingTime,DeltaencodingWriteTime=0, DeltaRencodingReadTime=0 ,DeltaRencodingTime=0,DeltaRencodingWriteTime=0;
		System.out.println("DeltaencodingReadTime   :"+DeltaencodingReadTime+"  mis");
		System.out.println("DeltaencodingTime   :"+DeltaencodingTime+"  mis");
		System.out.println("DeltaencodingWriteTime   :"+DeltaencodingWriteTime+"  mis");
		System.out.println("DeltaRencodingReadTime   :"+DeltaRencodingReadTime+"  mis");
		System.out.println("DeltaRencodingTime   :"+DeltaRencodingTime+"  mis");
		System.out.println("DeltaRencodingWriteTime   :"+DeltaRencodingWriteTime+"  mis");
		System.out.println("total  time :  "+(finalTime-startTime)+"  mis");
		long  encodingtotalLong=0 ,  revrsetotalLong=0 ;
		for(int j=0 ;j<length;j++){
			  revrsetotalLong= revrsetotalLong+new File(revrseEncodingstr[j]).length();
			encodingtotalLong=encodingtotalLong+new File(encodinstr[j]).length()+new File(dictionaryStr[j]).length();
		}

		System.out.println("encodingtotalLong  :  "+encodingtotalLong+"  /1024/1024  "+encodingtotalLong/1024/1024);
		System.out.println("revrsetotalLong  :  "+revrsetotalLong+"  /1024/1024  "+revrsetotalLong/1024/1024);
	}    

}
