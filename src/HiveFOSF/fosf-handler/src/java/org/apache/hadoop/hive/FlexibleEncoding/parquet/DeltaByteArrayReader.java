package org.apache.hadoop.hive.mastiffFlexibleEncoding.parquet;
/*
 * adapt from  parquet
 *
 */
import java.io.IOException;


/**
 * Reads binary data written by {@link DeltaByteArrayWriter}
 * 
 * @author Aniket Mokashi
 *
 */
public class DeltaByteArrayReader extends ValuesReader {
	private ValuesReader prefixLengthReader;
	private ValuesReader suffixReader;

	private Binary previous;
	private byte[] out = new byte[1];
	public DeltaByteArrayReader() {
		this.prefixLengthReader = new DeltaBinaryPackingValuesReader();
		this.suffixReader = new DeltaLengthByteArrayValuesReader();
		this.previous = Binary.fromByteArray(new byte[0]);
	}

	@Override
	public void initFromPage(int valueCount, byte[] page, int offset)
			throws IOException {
		prefixLengthReader.initFromPage(valueCount, page, offset);
		int next = prefixLengthReader.getNextOffset();
		suffixReader.initFromPage(valueCount, page, next);	
	}

	@Override
	public void skip() {
		prefixLengthReader.skip();
		suffixReader.skip();
	}

	@Override
	public Binary readBytes() {
		int prefixLength = prefixLengthReader.readInteger();
		// This does not copy bytes
		Binary suffix = suffixReader.readBytes();
		int length = prefixLength + suffix.length();

		// We have to do this to materialize the output
		if(prefixLength != 0) {
			byte[] out = new byte[length];
			System.arraycopy(previous.getBytes(), 0, out, 0, prefixLength);
			System.arraycopy(suffix.getBytes(), 0, out, prefixLength, suffix.length());
			previous =  Binary.fromByteArray(out);
		} else {
			previous = suffix;
		}
		return previous;
	}

	public byte readByte() {
		int prefixLength = prefixLengthReader.readInteger();
		// This does not copy bytes
		Binary suffix = suffixReader.readBytes();
	//	int length = prefixLength + suffix.length();
	// 	System.out.println("//////   "+prefixLength);
	 //	System.out.println("//////   "+suffix.length());
//		if(prefixLength!=1){
//       	System.out.println("111");
//      }
		// We have to do this to materialize the output
		if(prefixLength != 0) {
		//	byte[] out = new byte[length];
			System.arraycopy(previous.getBytes(), 0, out, 0, prefixLength);
			System.arraycopy(suffix.getBytes(), 0, out, prefixLength, suffix.length());
			previous =  Binary.fromByteArray(out);
		} else {
			previous = suffix;
		}
		return previous.getBytes()[0];
	}
}
