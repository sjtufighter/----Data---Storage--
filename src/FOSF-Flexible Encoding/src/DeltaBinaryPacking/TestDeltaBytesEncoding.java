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
	 public static void prepare() throws IOException {
		    FileOutputStream  fos =new FileOutputStream(new File("/home/wangmeng/DeltaByteSourceFile"));
		    DataOutputStream  dos=new   DataOutputStream(fos);
			  Random random = new Random();
		   // data = new int[100000 * blockSize];
		    data = new byte[1000 * blockSize];
		    System.out.println(data.length);
		    random.nextBytes(data);
		    for (int i = 0; i < data.length; i++) {
		      
		  //   fos.write(data[i]);
		     dos.writeInt(data[i]);
		   
		  //    dos.write(data[i]);
		    }
		    
		    System.out.println(dos.size());
		    dos.flush();
		    dos.close();
		    for (int i = 0; i < data.length; i++) {
		    	  
		        if(i>5800&&i<5810){
		       	 System.out.println(data[i]);
		    	
		        }
		    	
		    
		   //   rle.writeInteger(data[i]);
		    }
//		    fos.flush();
//		    fos.close();
		//    ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100);
		    DeltaByteArrayWriter  delta=new   DeltaByteArrayWriter(100);
		    
		   // ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);
		    delta.writeBytes(data);
		 
		   deltaBytes = delta.getBytes().toByteArray();
		   
		 System.out.println("deltaBytes"+deltaBytes.length);
		    System.out.println("rong liang   "+ delta.memUsageString("boas"));
		  //  rleBytes = rle.getBytes().toByteArray();
		  }
	
	 public static void readingDelta() throws IOException {
		  //  for (int j = 0; j < 10; j++) {

		 DeltaByteArrayReader reader = new DeltaByteArrayReader() ;
		     // ByteArrayOutputStream biss=new ByteArrayOutputStream();
		      File  file=   new File("/home/wangmeng/encodingFile");
		      
		      FileInputStream fis=new FileInputStream(file);
		      DataInputStream dis=new  DataInputStream(fis);
		     
		    
		     System.out.println("length"+file.length());
		     byte[]  bytes=new byte[ (int) file.length()];
		     
		      dis.readFully(bytes);
		      fis.close();
		      dis.close();
		      reader.initFromPage(1000 * blockSize, bytes,0);
		  Binary binary=    reader.readBytes();
		  byte[] result=binary.getBytes() ;
		  
		  for (int i = 0; i < data.length; i++) {
	    	  
		        if(i>5800&&i<5810){
		       	 System.out.println(result[i]);
		    	
		        }
		  }
		  
		   //   System.out.println(bytes[deltaBytes.length]);
		   //  ByteArrayInputStream bis=new ByteArrayInputStream(buf);
		      
		   //   readData(reader, deltaBytes);
		   // }
		    
		    System.out.println("run over");
		  }

	

	
	
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		prepare();
		readingDelta();
	}

}
