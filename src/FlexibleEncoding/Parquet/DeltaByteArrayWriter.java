package FlexibleEncoding.Parquet;


/*
 * adapt from  parquet
 *
 */


/**
 * Write prefix lengths using delta encoding, followed by suffixes with Delta length byte arrays
 * <pre>
 *   {@code
 *   delta-length-byte-array : prefix-length* suffixes*
 *   } 
 * </pre>
 *
 */
public class DeltaByteArrayWriter extends ValuesWriter{

	private ValuesWriter prefixLengthWriter;
	private ValuesWriter suffixWriter;
	private byte[] previous;
	private  byte[] vb=new byte[1];
	public DeltaByteArrayWriter(int initialCapacity) {
		this.prefixLengthWriter = new DeltaBinaryPackingValuesWriter(128, 4, initialCapacity);
		this.suffixWriter = new DeltaLengthByteArrayValuesWriter(initialCapacity);
		this.previous = new byte[0];
	}

	@Override
	public long getBufferedSize() {
		return prefixLengthWriter.getBufferedSize() + suffixWriter.getBufferedSize();
	}

	@Override
	public BytesInput getBytes() {
		return BytesInput.concat(prefixLengthWriter.getBytes(), suffixWriter.getBytes());
	}

	@Override
	public Encoding getEncoding() {
		return Encoding.DELTA_BYTE_ARRAY;
	}

	@Override
	public void reset() {
		prefixLengthWriter.reset();
		suffixWriter.reset();
	}

	@Override
	public long getAllocatedSize() {
		return prefixLengthWriter.getAllocatedSize() + suffixWriter.getAllocatedSize();
	}

	@Override
	public String memUsageString(String prefix) {
		prefix = prefixLengthWriter.memUsageString(prefix);
		return suffixWriter.memUsageString(prefix + "  DELTA_STRINGS");
	}
	public void writeBytes(byte v) {
		int i = 0;

		vb[i]=v ;
		int length = previous.length < vb.length ? previous.length : vb.length;
		//    length=vb.length;
		//    for(i = 0; (i < length) && (previous[i] == vb[i]); i++);
		//	    for( int j = 0 ;j < length ;j++){
		//	    	  prefixLengthWriter.writeInteger(i);
		//	  	    suffixWriter.writeBytes(Binary.fromByteArray(vb, i, vb.length - i));
		//	    }
		for(i = 0; (i < length) && (previous[i] == vb[i]); i++);
		prefixLengthWriter.writeInteger(i);
		suffixWriter.writeBytes(Binary.fromByteArray(vb, i, vb.length - i));
		previous = vb;
	}

	@Override
	public void writeBytes(Binary v) {
		int i = 0;
		byte[] vb = v.getBytes();
		int length = previous.length < vb.length ? previous.length : vb.length;
		for(i = 0; (i < length) && (previous[i] == vb[i]); i++);
		prefixLengthWriter.writeInteger(i);
		suffixWriter.writeBytes(Binary.fromByteArray(vb, i, vb.length - i));
		previous = vb;
	}
}
