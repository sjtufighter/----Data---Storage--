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
	public static  int fileLong ;
	static java.util.Calendar c=java.util.Calendar.getInstance();    
    static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒");    
	public static void prepare(String[] s) throws IOException {
		System.out.println("intdelta:  begin write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length()/4;
		System.out.println("filelong  "+fileLong);
		//data = new byte[(int) file.length()];
		//dis.readFully(data);
		
		
		 
		ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, (int)file.length()/10);
		// ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);

		for (int i = 0; i < fileLong; i++) {
			delta.writeInteger(dis.readInt());
		}
		dis.close();
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}
		//deltaBytes = delta.getBytes().toByteArray();
		delta.getBytes().writeToDisk(str);
		 System.out.println("intdelta:  finish  write to deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		
	}

	public static void readingDelta(String[] s) throws IOException {
		//  for (int j = 0; j < 10; j++) {
		System.out.println("intdelta:  begin read  from  deltaencoding :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
		// ByteArrayOutputStream biss=new ByteArrayOutputStream();
		File  file=   new File(s[1]);
		System.out.println("length"+file.length());
		FileInputStream fis=new FileInputStream(file);
		DataInputStream dis=new  DataInputStream(fis);


		
		byte[]  bytes=new byte[ (int) file.length()];
		System.out.println("length"+bytes.length);
		dis.readFully(bytes);
		fis.close();
		dis.close();
		//   System.out.println(bytes[deltaBytes.length]);
		//  ByteArrayInputStream bis=new ByteArrayInputStream(buf);

		//   readData(reader, deltaBytes);
		readData(reader, bytes,s);
		// }

		System.out.println("intdelta: finish read  from  deltaencoding  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
	}
	private static void readData(ValuesReader reader, byte[] delta,String[] s) throws IOException {
	System.out.println("filelong  "+fileLong);	
	//fileLong=(int) (new File(s[0]).length()/4);
	//fileLong=10 ;
	
		reader.initFromPage(fileLong, delta, 0);
		
		System.out.println("filelong  "+fileLong);	
		int  count=0 ;
		for (int i = 0; i < fileLong; i++) {
		 int m=	reader.readInteger();
		 count ++ ;
//		 if(count<Integer.parseInt(s[2])){
//			 System.out.println("file data "+ m);
//		}
		
		
	}
		System.out.println("filelong count "+count);	
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub


				prepare(args);
		   readingDelta(args);
	}

}

