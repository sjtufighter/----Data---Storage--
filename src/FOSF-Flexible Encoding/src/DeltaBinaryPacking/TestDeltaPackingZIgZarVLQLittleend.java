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
	public static int[] data;
	//  @Rule
	//  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

	//  @BeforeClass
	public static void prepare() throws IOException {
		FileOutputStream  fos =new FileOutputStream(new File("/home/wangmeng/DeltaIntSourceFile"));
		DataOutputStream  dos=new   DataOutputStream(fos);
		Random random = new Random();
		// data = new int[100000 * blockSize];
		data = new int[1000 * blockSize];
		System.out.println(data.length);
		for (int i = 0; i < data.length; i++) {
			data[i] = random.nextInt(100) - 200;
			//   fos.write(data[i]);
			dos.writeInt(data[i]);

			//    dos.write(data[i]);
		}
		System.out.println(dos.size());
		dos.flush();
		dos.close();
		//    fos.flush();
		//    fos.close();
		ValuesWriter delta = new DeltaBinaryPackingValuesWriter(blockSize, miniBlockNum, 100);
		// ValuesWriter rle = new RunLengthBitPackingHybridValuesWriter(32, 100);

		for (int i = 0; i < data.length; i++) {

			if(i>5800&&i<5810){
				System.out.println(data[i]);
			}

			delta.writeInteger(data[i]);
			//   rle.writeInteger(data[i]);
		}
		deltaBytes = delta.getBytes().toByteArray();
		//System.out.println("deltaBytes"+deltaBytes.length);
		System.out.println("rong liang   "+ delta.memUsageString("boas"));
		//  rleBytes = rle.getBytes().toByteArray();
	}

	//  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
	//  @Test
	public static void readingDelta() throws IOException {
		//  for (int j = 0; j < 10; j++) {

		DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
		// ByteArrayOutputStream biss=new ByteArrayOutputStream();
		File  file=   new File("/home/wangmeng/encodingFile");

		FileInputStream fis=new FileInputStream(file);
		DataInputStream dis=new  DataInputStream(fis);


		System.out.println("length"+file.length());
		byte[]  bytes=new byte[ (int) file.length()];

		dis.readFully(bytes);
		fis.close();
		dis.close();
		//   System.out.println(bytes[deltaBytes.length]);
		//  ByteArrayInputStream bis=new ByteArrayInputStream(buf);

		//   readData(reader, deltaBytes);
		readData(reader, bytes);
		// }

		System.out.println("run over");
	}

	//  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 10)
	//  @Test
	public void readingRLE() throws IOException {
		for (int j = 0; j < 10; j++) {

			ValuesReader reader = new RunLengthBitPackingHybridValuesReader(32);
			readData(reader, rleBytes);
		}
	}

	private static void readData(ValuesReader reader, byte[] delta) throws IOException {
		reader.initFromPage(data.length, delta, 0);
		for (int i = 0; i < data.length; i++) {
			reader.readInteger();
		}
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		int[] datza=new int[1000];
		int count=0 ;
		Random  random =new Random();
		for (int i = 0; i <1000; i++) {
			datza[i] = random.nextInt();
			if(datza[i]<0){
				System.out.println( datza[i]);
				count++;
			}

			//   fos.write(data[i]);
			//   dos.writeInt(data[i]);

			//    dos.write(data[i]);
		}

		System.out.println( "//////////////"+count);

		//		prepare();
		//   readingDelta();
	}

}

