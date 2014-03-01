package ORC;

/**
adapted from ORC
@author wangmeng
 */


import org.junit.Test;

import DeltaBinaryPacking.DictionaryValuesWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class TestRunLengthByte {
	static java.util.Calendar c=java.util.Calendar.getInstance();    
	static java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒"); 
	public static  int fileLong ;

	public static void runSeekTest(String[] s ,CompressionCodec codec) throws Exception {
		System.out.println("intORCRLE :  begin write to intORCRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		TestInStream.OutputCollector collect = new TestInStream.OutputCollector();

//RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 100,
   //     null, collect));
		RunLengthByteWriter out = new RunLengthByteWriter(new OutStream("test", 1000, codec, collect));
		File file=new File(s[0]) ;
		FileInputStream  fis =new FileInputStream(file);
		DataInputStream  dis=new   DataInputStream(fis);
		fileLong=(int) file.length();
		byte[] initbytes=new byte[fileLong];
		System.out.println("fileLong  "+fileLong);
		for (int i = 0; i < fileLong; i++) {
		out.write(dis.readByte());
		//cw.writeInteger(dis.readInt());
	//	out.write(dis.read(initbytes));
		
		}

		dis.close();
		out.flush();
		String[]  str =new String[s.length-1];
		for(int i=0 ;i<s.length-1;i++){
			str[i]=s[i+1];
		}



		//		// TestInStream.PositionCollector[] positions =new TestInStream.PositionCollector[4096];
		//		Random random = new Random(99);
		//		int[] junk = new int[2048];
		//		for(int i=0; i < junk.length; ++i) {
		//			junk[i] = random.nextInt();
		//			
		//		}
		//		FileOutputStream Sourcefos=new FileOutputStream(new File("/home/wangmeng/SourceORCRLE"));
		////		DataOutputStream  Sourcedos =new DataOutputStream(Sourcefos);
		//		for(int i=0; i < 4096; ++i) {
		//			//   positions[i] = new TestInStream.PositionCollector();
		//			//  out.getPosition(positions[i]);
		//			// test runs, incrementing runs, non-runs
		//			if (i < 1024) {
		//				out.write(i/4);
		//				Sourcedos.writeInt(i/4);
		////			} else if (i < 2048) {
		////				out.write(2*i);
		////				Sourcedos.writeInt(2*i);
		//			} else {
		////				out.write(junk[i-2048]);
		////				Sourcedos.writeInt(junk[i-2048]);
		//				out.write(2*i);
		//				
		//				Sourcedos.writeInt(2*i);
		//			}
		//		}




		//	System.out.println("54  "+collect.buffer.size());

		// System.out.println("56  "+inBuf.array().length);
		FileOutputStream fos=new FileOutputStream(new File(s[1]));
		DataOutputStream  dos =new DataOutputStream(fos);
		collect.buffer.write(dos, 0, collect.buffer.size());
		dos.close();

		System.out.println("intORCRLE :  finish  write to intORCRLE :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		/////added  by  wm to text read file from disk 
		System.out.println("intORCRLE :  begin read  from intORCRLE  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));
		DynamicByteArray  dynamicBuffer = new DynamicByteArray();
		File readfile=new File(s[1]);
		FileInputStream  readfis=new FileInputStream(readfile);
		DataInputStream  readdis =new DataInputStream(readfis);
		byte[] bytes=new byte[(int)readfile.length()];			
		readdis.readFully(bytes);
		readdis.close();
		dynamicBuffer.add(bytes, 0, bytes.length);

		ByteBuffer inBuf = ByteBuffer.allocate(dynamicBuffer.size());
		//  System.out.println("56  "+inBuf.getInt());
		dynamicBuffer.setByteBuffer(inBuf, 0, dynamicBuffer.size());

		inBuf.flip();

		RunLengthByteReader in = new RunLengthByteReader(InStream.create
				("test", inBuf, codec, (int)readfile.length()));
		int  count=0 ;
		
		for(int i=0; i < fileLong; ++i) {

			byte  x = (byte) in.next();
			count ++ ;
		}
		System.out.println("total  count "+ count);
		inBuf.clear();

		System.out.println("intORCRLE : finish read  from  intORCRLE  :  " +new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime()));

	}

	@Test
	public static void testUncompressedSeek(String[] s) throws Exception {
		runSeekTest(s,null);
	}

	@Test
	public static void testCompressedSeek(String[] s) throws Exception {
		//   runSeekTest(new ZlibCodec());
		runSeekTest(s,new SnappyCodec());
	}

	//	@Test
	//	public void testSkips() throws Exception {
	//		TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
	//		RunLengthIntegerWriter out = new RunLengthIntegerWriter(
	//				new OutStream("test", 100, null, collect), true);
	//		for(int i=0; i < 2048; ++i) {
	//			if (i < 1024) {
	//				out.write(i);
	//			} else {
	//				out.write(256 * i);
	//			}
	//		}
	//		out.flush();
	//		ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
	//		collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
	//		inBuf.flip();
	//		RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
	//				("test", inBuf, null, 100), true);
	//		for(int i=0; i < 2048; i += 10) {
	//			int x = (int) in.next();
	//			if (i < 1024) {
	//				assertEquals(i, x);
	//			} else {
	//				assertEquals(256 * i, x);
	//			}
	//			if (i < 2038) {
	//				in.skip(9);
	//			}
	//			in.skip(0);
	//		}
	//	}


	public  static  void main(String[] args) throws Exception{
		testUncompressedSeek(args) ;
		////		System.out.println("size  "+args.length);
		//   // testCompressedSeek();
		////		System.out.println("run  over");
		//	ArrayList  al=new ArrayList<String>(20);
		//	System.out.println(al.size());
		//	byte[]  bytes =new byte[800];
		//	System.out.println(bytes.length);
	}
}
