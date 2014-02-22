package ORC;

/**
adapted from ORC
@author wangmeng
 */

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class TestRunLengthInteger {

	public static void runSeekTest(CompressionCodec codec) throws Exception {
		TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
		RunLengthIntegerWriter out = new RunLengthIntegerWriter(
				new OutStream("test", 1000, codec, collect), true);

		// TestInStream.PositionCollector[] positions =new TestInStream.PositionCollector[4096];
		Random random = new Random(99);
		int[] junk = new int[2048];
		for(int i=0; i < junk.length; ++i) {
			junk[i] = random.nextInt();
			
		}
		FileOutputStream Sourcefos=new FileOutputStream(new File("/home/wangmeng/SourceORCRLE"));
		DataOutputStream  Sourcedos =new DataOutputStream(Sourcefos);
		for(int i=0; i < 4096; ++i) {
			//   positions[i] = new TestInStream.PositionCollector();
			//  out.getPosition(positions[i]);
			// test runs, incrementing runs, non-runs
			if (i < 1024) {
				out.write(i/4);
				Sourcedos.writeInt(i/4);
//			} else if (i < 2048) {
//				out.write(2*i);
//				Sourcedos.writeInt(2*i);
			} else {
//				out.write(junk[i-2048]);
//				Sourcedos.writeInt(junk[i-2048]);
				out.write(2*i);
				
				Sourcedos.writeInt(2*i);
			}
		}
		out.flush();
		Sourcedos.close();
		System.out.println("54  "+collect.buffer.size());
		
		// System.out.println("56  "+inBuf.array().length);
		FileOutputStream fos=new FileOutputStream(new File("/home/wangmeng/ORCRLE"));
		DataOutputStream  dos =new DataOutputStream(fos);
		collect.buffer.write(dos, 0, collect.buffer.size());
		dos.close();
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		/////added  by  wm to text read file from disk 
		 DynamicByteArray  dynamicBuffer = new DynamicByteArray();
		 File file=new File("/home/wangmeng/ORCRLE");
		 FileInputStream  fis=new FileInputStream(file);
			DataInputStream  dis =new DataInputStream(fis);
			byte[] bytes=new byte[(int) file.length()];
			
	     dis.readFully(bytes);
	     dis.close();
     dynamicBuffer.add(bytes, 0, bytes.length);
	   // collect.buffer.clear();
	//	ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
		ByteBuffer inBuf = ByteBuffer.allocate(dynamicBuffer.size());
		//  System.out.println("56  "+inBuf.getInt());
		dynamicBuffer.setByteBuffer(inBuf, 0, dynamicBuffer.size());
		
	//	collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
		//  System.out.println("57  "+inBuf.array().length);
		// System.out.println("57  "+inBuf.getInt());
		inBuf.flip();
		//  System.out.println("58  "+inBuf.array().length);
		//    System.out.println("58  "+inBuf.getInt());
		RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
				("test", inBuf, codec, 1000), true);

		for(int i=0; i < 4096; ++i) {
			
			int x = (int) in.next();


//			if(i<10){
//				System.out.println(x);
//			}


//			if (i < 1024) {
//				assertEquals(i/4, x);
//			} else if (i < 2048) {
//				assertEquals(2*i, x);
//			} else {
//				assertEquals(junk[i-2048], x);
//			}
		}
		
	inBuf.clear();
		//    for(int i=2047; i >= 0; --i) {
			//      in.seek(positions[i]);
		//      int x = (int) in.next();
		//      if (i < 1024) {
		//        assertEquals(i/4, x);
		//      } else if (i < 2048) {
		//        assertEquals(2*i, x);
		//      } else {
		//        assertEquals(junk[i-2048], x);
		//      }
		//    }
	}

	@Test
	public static void testUncompressedSeek() throws Exception {
		runSeekTest(null);
	}

	@Test
	public static void testCompressedSeek() throws Exception {
	//   runSeekTest(new ZlibCodec());
		runSeekTest(new SnappyCodec());
	}

	@Test
	public void testSkips() throws Exception {
		TestInStream.OutputCollector collect = new TestInStream.OutputCollector();
		RunLengthIntegerWriter out = new RunLengthIntegerWriter(
				new OutStream("test", 100, null, collect), true);
		for(int i=0; i < 2048; ++i) {
			if (i < 1024) {
				out.write(i);
			} else {
				out.write(256 * i);
			}
		}
		out.flush();
		ByteBuffer inBuf = ByteBuffer.allocate(collect.buffer.size());
		collect.buffer.setByteBuffer(inBuf, 0, collect.buffer.size());
		inBuf.flip();
		RunLengthIntegerReader in = new RunLengthIntegerReader(InStream.create
				("test", inBuf, null, 100), true);
		for(int i=0; i < 2048; i += 10) {
			int x = (int) in.next();
			if (i < 1024) {
				assertEquals(i, x);
			} else {
				assertEquals(256 * i, x);
			}
			if (i < 2038) {
				in.skip(9);
			}
			in.skip(0);
		}
	}


	public  static  void main(String[] args) throws Exception{
 //	testUncompressedSeek() 
		System.out.println("size  "+args.length);
testCompressedSeek();
//		System.out.println("run  over");
	ArrayList  al=new ArrayList<String>(20);
	System.out.println(al.size());
	byte[]  bytes =new byte[800];
	System.out.println(bytes.length);
	}
}
