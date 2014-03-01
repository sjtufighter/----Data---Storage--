package DeltaBinaryPacking;
/*
 * this is used for test  various encoding  from Parquet and ORC file . also used to  test some new  combinations encodings  of basic encoding ways from Parquet and ORC ,which is combineed  by  wangmeng
 * @author  wangmeng
 */
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class TestDeltaBytesEncoding {
	public static int blockSize = 128;
	public static int miniBlockNum = 4;
	public static byte[] deltaBytes;
	public static byte[] rleBytes;
	public static byte[] data;
	public static  int fileLong ;
	static java.util.Calendar c=java.util.Calendar.getInstance();    
	static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒");    

	public static void prepare(String[] s) throws IOException {
		System.out.println("bytedelta:  begin write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length();
		System.out.println("fileLong  "+fileLong);
		data = new byte[(int) file.length()];
		dis.readFully(data);

		//    ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100);
		DeltaByteArrayWriter  delta=new   DeltaByteArrayWriter((int)file.length()/10);

		// ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);
	delta.writeBytes(data);
//		for(int i=0 ;i<fileLong ;i++){
//			delta.writeByte(dis.readByte());
//		}
	//delta.writeBytes(v);
		
		
	//	delta.writeBytes(v);
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}
		delta.getBytes().writeToDisk(str);


		System.out.println("bytedelta:  finish  write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		//		 System.out.println("deltaBytes"+deltaBytes.length);
		//		    System.out.println("rong liang   "+ delta.memUsageString("boas"));
		//  rleBytes = rle.getBytes().toByteArray();
	}

	public static void readingDelta(String[] s) throws IOException {
		//  for (int j = 0; j < 10; j++) {
		System.out.println("bytedelta:  begin read  from  deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		// ByteArrayOutputStream biss=new ByteArrayOutputStream();
		File  file=   new File(s[1]);

		FileInputStream fis=new FileInputStream(file);
		DataInputStream dis=new  DataInputStream(fis);


		//	System.out.println("length"+file.length());
		byte[]  bytes=new byte[ (int) file.length()];

		dis.readFully(bytes);
		fis.close();
		dis.close();

		DeltaByteArrayReader reader = new DeltaByteArrayReader() ;
		reader.initFromPage(fileLong, bytes,0);
		Binary binary=    reader.readBytes();
		byte[] result=binary.getBytes() ;
		int count=0 ;
		System.out.println("bytedelta: finish read  from  deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		for (int i = 0; i < result.length; i++) {


			byte m=    result[i];
			count++ ;
		}  
		System.out.println("total count "+count);
		System.out.println("bytedelta: finish read  from  deltaencoding and after bianli  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		//   System.out.println(bytes[deltaBytes.length]);
		//  ByteArrayInputStream bis=new ByteArrayInputStream(buf);

		//   readData(reader, deltaBytes);
		// }


	}






	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		prepare(args);
		readingDelta(args);
	}

}
