package FlexibleEncoding.Parquet;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapred.FileOutputCommitter;

public class IO {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
//     DataOutputStream  dos =new DataOutputStream(new FileOutputStream(new File("/home/wangmeng/zidian")));
//     dos.writeInt(1000);;
//     dos.writeInt(78);
//     dos.close();
     
     DataInputStream  dis =new DataInputStream(new FileInputStream(new File("/home/wangmeng/zidian")));
     System.out.println(new File("/home/wangmeng/zidian").length());
     System.out.println(dis.readInt());
	}

}
