package DeltaBinaryPacking;

import java.io.IOException;

public class TestPlainEncoding {

	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		PlainValuesWriter  pvw = new PlainValuesWriter(1000) ;
		for (int i=0 ;i<100 ;i++){
			pvw.writeInteger(i);
		}
	System.out.println(pvw.getBytes().toByteArray().length);	
		
	}

}
