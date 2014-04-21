package FlexibleEncoding.Parquet;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class tmp {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		  java.util.Calendar c=java.util.Calendar.getInstance();    
	        java.text.SimpleDateFormat f=new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒");    
	        System.out.println(f.format(c.getTime()));     
//   System.out.println(System.currentTimeMillis());
   double  m=1.3 ;
//   for ( int i= 0 ;i<100000000;i++){
//	   m=m/Math.random()*Math.random() ;
//   }
   System.out.println(System.currentTimeMillis());
   System.out.println(f.format(c.getTime()));     
   System.out.println(new java.text.SimpleDateFormat("yyyy年MM月dd日hh时mm分ss秒").format(java.util.Calendar.getInstance().getTime())); 
   File file=new File("/home/wangmeng/lll/mmm");
 
   FileOutputStream fos=new FileOutputStream(file);
	DataOutputStream  dos =new DataOutputStream(fos);
    dos.close();
	}

}
