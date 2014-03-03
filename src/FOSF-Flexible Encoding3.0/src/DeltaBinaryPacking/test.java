package DeltaBinaryPacking;

import java.awt.List;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class test {
	private static  int[] nums ;
	public static int[] writeToDat(String path) {
		File file = new File(path);
		ArrayList list = new ArrayList();
		int[] nums = null;
		try {
			BufferedReader bw = new BufferedReader(new FileReader(file));
			String line = null;
			//因为不知道有几行数据，所以先存入list集合中
			while((line = bw.readLine()) != null){
				list.add(line);
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//确定数组长度
		nums = new int[list.size()];
		for(int i=0;i<list.size();i++){
			String s = (String) list.get(i);
			nums[i] = Integer.parseInt(s);
		}
		return nums;
	}



	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//	File binaryfile =new File("/home/hduser/zengdan/wangmeng/encodingTest/binaryfile/l_orderkey");
		File binaryfile =new File(args[1]);	
		System.out.println(args[1]);
		FileOutputStream  fos =new FileOutputStream( binaryfile);
		DataOutputStream  dos=new   DataOutputStream(fos);
		//	 File  file=new File("/home/hduser/zengdan/wangmeng/encodingTest/l_orderkey");
		File  file=new File(args[0]);
		System.out.println(args[0]);
		File[] files=file.listFiles() ;
		for(int j=0;j<files.length;j++){
			nums = writeToDat(files[j].getAbsolutePath());
			for(int i=0;i<nums.length;i++){
				//    System.out.println(nums[i]);
				dos.writeInt(nums[i]);  
			}

		}

		dos.close();
		//		  System.out.println("/////////////////////////////////////////");
		//		  FileInputStream  fis =new FileInputStream(file);
		//		  DataInputStream  dis=new   DataInputStream(fis);
		//		  for(int i=0;i<nums.length;i++){
		//		      System.out.println(dis.readInt());
		//			  //dos.writeInt(nums[i]);  
		//}
	}

}