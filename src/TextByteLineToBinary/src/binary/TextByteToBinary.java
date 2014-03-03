package binary;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;


public class TextByteToBinary {


	private static  byte[] nums ;
	private static int count=0 ;
	public static byte[] writeToDat(String path) {
		File file = new File(path);
		ArrayList list = new ArrayList();
		byte[] nums = null;
		try {
			BufferedReader bw = new BufferedReader(new FileReader(file));
			String line = null;
		
			while((line = bw.readLine()) != null){
				list.add(line);
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//确定数组长度
		nums = new byte[list.size()];
		for(int i=0;i<list.size();i++){
			String s = (String) list.get(i);
			
			
			if(s.length()!=0){
				nums[i] = Byte.parseByte(s);
			}else{
				
				System.out.println("s.length()==0"+"s =  "+ s+"count=  "+count);
				count++ ;
				nums[i]=(byte)0;
			}
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
		int l=0;
		System.out.println(args[0]);
		File[] files=file.listFiles() ;
		System.out.println("/////////////////////////////////////page numberfiles.length  "+files.length);;
		for(int j=0;j<files.length;j++){
			nums = writeToDat(files[j].getAbsolutePath());
			System.out.println("/////////////////////////////////////page number "+j);
			//	for(int i=0;i<nums.length;i++){
			//    System.out.println(nums[i]);
			dos.flush();
			dos.write(nums);
			l++ ;
			if(l==1000000){
				dos.flush();
				l=0;
			}
			//	}

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
