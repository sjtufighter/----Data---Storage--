package DeltaBinaryPacking;

/*
 * this is used for test  various encoding  from Parquet and ORC file . also used to  test some new  combinations encodings  of basic encoding ways from Parquet and ORC ,which is combineed  by  wangmeng
 * @author  wangmeng
 */
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

//@AxisRange(min = 0, max = 1)
//@BenchmarkMethodChart(filePrefix = "benchmark-encoding-reading-random")
public class TestDeltaPackingZIgZarVLQLittleend {
	public static int blockSize = 128;
	public static int miniBlockNum = 4;
	public static byte[] deltaBytes;
	public static byte[] rleBytes;
	public static  int[] data;
	public static  int fileLong =0 ,count =0 ,runcount=0 ;
	public static   long startTime ,encodingReadTime ,encodingTime,encodingWriteTime, RencodingReadTime ,RencodingTime,RencodingWriteTime, finalTime;
	public static   long DeltaencodingReadTime=0 ,DeltaencodingTime,DeltaencodingWriteTime=0, DeltaRencodingReadTime=0 ,DeltaRencodingTime=0,DeltaRencodingWriteTime=0;
	static java.util.Calendar c=java.util.Calendar.getInstance();    
	static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒");    
	public static void prepare(String[] s) throws IOException {
		//	System.out.println("intdelta:  begin write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		encodingReadTime=System.currentTimeMillis() ;
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length()/4;
		//		System.out.println("filelong  "+fileLong);
		int[] initvalue=new int[fileLong] ;
		for (int i = 0; i < fileLong; i++) {
			initvalue[i]=dis.readInt(); 
		}
		dis.close();
		encodingTime=System.currentTimeMillis() ;
		DeltaencodingReadTime=DeltaencodingReadTime+encodingTime-encodingReadTime ;	

		ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, (int)file.length()/10);
		// ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);

		for (int i = 0; i < fileLong; i++) {
			delta.writeInteger(initvalue[i]);
		}
		BytesInput bi=delta.getBytes() ;
		encodingWriteTime=System.currentTimeMillis() ;
		
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}
		//deltaBytes = delta.getBytes().toByteArray();
		//long tmp=System.currentTimeMillis();

		//System.out.println("tome "+(System.currentTimeMillis()-tmp));
		//	delta.getBytes().writeToDisk(str);
		long tmp=bi.writeToDisk(str);
		DeltaencodingTime=DeltaencodingTime+encodingWriteTime-encodingTime +tmp ;
		RencodingReadTime=System.currentTimeMillis() ;
		DeltaencodingWriteTime=DeltaencodingWriteTime+RencodingReadTime-encodingWriteTime-tmp ;
		//	System.out.println("intdelta:  finish  write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 

	}

	public static void readingDelta(String[] s) throws IOException {
		//  for (int j = 0; j < 10; j++) {
		//	System.out.println("intdelta:  begin read  from  deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));

		DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
		// ByteArrayOutputStream biss=new ByteArrayOutputStream();
		File  file=   new File(s[1]);
		//	System.out.println("length"+file.length());
		FileInputStream fis=new FileInputStream(file);
		DataInputStream dis=new  DataInputStream(fis);
		byte[]  bytes=new byte[ (int) file.length()];
		//	System.out.println("length"+bytes.length);
		dis.readFully(bytes);
		fis.close();
		dis.close();
		RencodingTime=System.currentTimeMillis() ;
		DeltaRencodingReadTime=DeltaRencodingReadTime+RencodingTime-RencodingReadTime;
		readData(reader, bytes,s);
		// }

		//System.out.println("intdelta: finish read  from  deltaencoding  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
	}
	private static void readData(ValuesReader reader, byte[] delta,String[] s) throws IOException {
		reader.initFromPage(fileLong, delta, 0);
		//		System.out.println("filelong  "+fileLong);	
		int[]  result=new int[fileLong];
		for (int i = 0; i < fileLong; i++) {
			result[i] =	reader.readInteger();
			count ++ ;
		}
		RencodingWriteTime=System.currentTimeMillis() ;
		DeltaRencodingTime=DeltaRencodingTime+RencodingWriteTime-RencodingTime;
		FileOutputStream    revrseencodingFis=new  FileOutputStream(new File(s[3])) ;
		DataOutputStream  revrseencodingDos=new DataOutputStream(revrseencodingFis) ;
		for(int i=0; i < fileLong; ++i) {
			//int x = (int) in.next();
			revrseencodingDos.writeInt(result[i]);
		}
		revrseencodingFis.close();
		revrseencodingDos.close();
		finalTime=System.currentTimeMillis() ;
		DeltaRencodingWriteTime=DeltaRencodingWriteTime+finalTime-RencodingWriteTime;

	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		File sourcefileDirectory =new File(args[0]);
		File[]  sourcefiles=    sourcefileDirectory.listFiles() ;
		int length=sourcefiles.length;
		String[] sourcestr=new String[length];
		String[] encodinstr=new String[length];
		//String[]  dictionaryStr=new String[length];
		String[]  revrseEncodingstr=new String[length];
		for (int i=0;i<sourcefiles.length;i++){
			sourcestr[i]=sourcefiles[i].getAbsolutePath() ;
			encodinstr[i]=args[1]+"/"+i  ;
			//	dictionaryStr[i]=args[3]+"/"+i  ;
			revrseEncodingstr[i]=args[3]+"/"+i  ;
			//	System.out.println(finalstr[i]);
		}
		String[]  runstr=new String[4] ;
		runstr[2]=args[2];
		startTime=System.currentTimeMillis() ;
		System.out.println("ORCRLE :  begin ORCRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		for (int i=0;i<length;i++){
			runstr[0]=sourcestr[i];
			runstr[1]=encodinstr[i] ;
			//runstr[3]=dictionaryStr[i];
			runstr[3]=revrseEncodingstr[i] ;
			prepare(runstr);
			readingDelta(runstr);
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
			encodingtotalLong=encodingtotalLong+new File(encodinstr[j]).length();
		}

		System.out.println("encodingtotalLong  :  "+encodingtotalLong+"  /1024/1024  "+encodingtotalLong/1024/1024);
		System.out.println("revrsetotalLong  :  "+revrsetotalLong+"  /1024/1024  "+revrsetotalLong/1024/1024);


	}

}

