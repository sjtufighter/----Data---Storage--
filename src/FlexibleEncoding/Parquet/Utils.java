package FlexibleEncoding.Parquet;

import java.io.IOException;
import java.util.Random;
//
//import parquet.column.values.RandomStr;
//import parquet.column.values.ValuesReader;
//import parquet.column.values.ValuesWriter;
//import parquet.io.api.Binary;

public class Utils {
	  private static Random randomLen = new Random();
	  private static RandomStr randomStr = new RandomStr(randomLen);
	  
	  public static String[] getRandomStringSamples(int numSamples, int maxLength) {
	    String[] samples = new String[numSamples];
	    
	    for (int i=0; i < numSamples; i++) {
	      int len = randomLen.nextInt(maxLength);
	      samples[i] = randomStr.get(len);
	    }
	    
	    return samples;
	  }
	  
	  public static void writeInts(ValuesWriter writer, int[] ints)
	      throws IOException {
	    for(int i=0; i < ints.length; i++) {
	      writer.writeInteger(ints[i]);
	    }
	  }

	  public static void writeData(ValuesWriter writer, String[] strings)
	      throws IOException {
	    for(int i=0; i < strings.length; i++) {
	      writer.writeBytes(Binary.fromString(strings[i]));
	    }
	  }
	  public   void writeEachData(ValuesWriter writer, String string)
        {
   //   for(int i=0; i < strings.length; i++) {
        writer.writeBytes(Binary.fromString(string));
     // }
    }
   
	  public  Binary[] readData(ValuesReader reader, byte[] data, int offset, int length)
	      throws IOException {
	    Binary[] bins = new Binary[length];
	    reader.initFromPage(length, data, 0);
	   
	    for(int i=0; i < length; i++) {
	      bins[i] = reader.readBytes();
	      if(i<10){
	        System.out.println("54   "+bins[i].toStringUsingUTF8());
	      }
	    }
	    return bins;
	  }
	  
	  
	  
	  public  Binary[] readData(ValuesReader reader, byte[] data, int length)
	      throws IOException {
	    return readData(reader, data, 0, length);
	  }
	  
	  
	  public static int[] readInts(ValuesReader reader, byte[] data, int offset, int length)
	      throws IOException {
	    int[] ints = new int[length];
	    reader.initFromPage(length, data, offset);
	    for(int i=0; i < length; i++) {
	      ints[i] = reader.readInteger();
	    }
	    return ints;
	  }
	  
	  public static int[] readInts(ValuesReader reader, byte[] data, int length)
	      throws IOException {
	    return readInts(reader, data, 0, length);
	  }
	}
