package FlexibleEncoding.Parquet;

import java.io.IOException;
import java.util.Arrays;

public class TestStringDeltaBianryPacking {

  //  @Rule
  //  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  static String[] values = Utils.getRandomStringSamples(10000, 32);
  static String[] sortedVals;
  static
  {
    sortedVals = Arrays.copyOf(values, values.length);
    Arrays.sort(sortedVals);
  }

  //	  @BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  //	  @Test
  public void benchmarkRandomStringsWithPlainValuesWriter() throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(64*1024);
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();

    Utils.writeData(writer, values);
    byte [] data = writer.getBytes().toByteArray();
  //  Binary[] bin = Utils.readData(reader, data, values.length);

    System.out.println("size " + data.length);
    //System.out.println("binsize " + bin.length);
  }

  //@BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  //	@Test
  public void benchmarkRandomStringsWithDeltaLengthByteArrayValuesWriter() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64*1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
  
    Utils.writeData(writer, values);
    byte [] data = writer.getBytes().toByteArray();
    Binary[] bin = new  Utils().readData(reader, data, values.length);
    System.out.println("size " + data.length);System.out.println("binsize " + bin.length);
    for(int i=0;i<5;i++){
      System.out.println("////////////////////"+bin[i].toString());
      System.out.println("56..................."+bin[i].toStringUsingUTF8());
    }


  }

  //@BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  //@Test
  public void benchmarkSortedStringsWithPlainValuesWriter() throws IOException {
    PlainValuesWriter writer = new PlainValuesWriter(64*1024);
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();

    Utils.writeData(writer, sortedVals);
    byte [] data = writer.getBytes().toByteArray();
    Binary[] bin = new Utils().readData(reader, data, values.length);
    System.out.println("size " + data.length);System.out.println("binsize " + bin.length);
  }

  //	@BenchmarkOptions(benchmarkRounds = 20, warmupRounds = 4)
  //@Test
  public void benchmarkSortedStringsWithDeltaLengthByteArrayValuesWriter() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64*1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, sortedVals);
    byte [] data = writer.getBytes().toByteArray();
    Binary[] bin = new  Utils().readData(reader, data, values.length);
    System.out.println("size " + data.length);System.out.println("binsize " + bin.length);
  }
  public static void  main(String[] str) throws IOException{
    TestStringDeltaBianryPacking    testString=new TestStringDeltaBianryPacking() ;
    testString.benchmarkRandomStringsWithDeltaLengthByteArrayValuesWriter();
    // testString.benchmarkRandomStringsWithPlainValuesWriter();

    // testString.benchmarkSortedStringsWithDeltaLengthByteArrayValuesWriter();
    // testString.benchmarkSortedStringsWithPlainValuesWriter();
  }
}
