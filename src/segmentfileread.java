package segmentfileread;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.hamcrest.core.IsSame;

import cn.ac.ncic.mastiff.io.segmentfile.PageMetaList;
import cn.ac.ncic.mastiff.io.segmentfile.PageMetaSection;
import cn.ac.ncic.mastiff.utils.Bytes;

public class FileSystemCat {
	static int[]  col0;
	static int[] col1 ;
	static int[] col2;
	static int[]  col3 ;
	static double[] col4 ;
	static double[]  col5 ;
	static double[] col6 ;
	static double[]  col7 ;
	static byte[]  col8 ;
	static byte[] col9 ;
	static int[]  col0Min;
	static int[] col1Min ;
	static int[] col2Min;
	static int[]  col3Min ;
	static double[] col4Min ;
	static double[]  col5Min ;
	static double[] col6Min ;
	static double[]  col7Min ;
	static byte[]  col8Min ;
	static byte[] col9Min ;
	static int[]  PageMetacol0;
	static int[] PageMetacol1 ;
	static int[] PageMetacol2;
	static int[]  PageMetacol3 ;
	static double[] PageMetacol4 ;
	static double[]  PageMetacol5 ;
	static double[] PageMetacol6 ;
	static double[]  PageMetacol7 ;
	static byte[]  PageMetacol8 ;
	static byte[] PageMetacol9 ;
	static int[]  PageMetacol0Min;
	static int[] PageMetacol1Min ;
	static int[] PageMetacol2Min;
	static int[]  PageMetacol3Min ;
	static double[] PageMetacol4Min ;
	static double[]  PageMetacol5Min ;
	static double[] PageMetacol6Min ;
	static double[]  PageMetacol7Min ;
	static byte[]  PageMetacol8Min ;
	static byte[] PageMetacol9Min ;
	static long[]  col0PageOffset,col1PageOffset,col2PageOffset,col3PageOffset,col4PageOffset;
	static int[]  col0Numpairs,col1Numpairs,col2Numpairs,col3Numpairs,col4Numpairs;
	static int[]  col0Seg=new int[8];
	static int[] col1Seg=new int[8]; ;
	static int[] col2Seg=new int[8];;
	static int[]  col3Seg=new int[8]; ;
	static double[] col4Seg=new double[8]; ;
	static double[]  col5Seg=new double[8]; ;
	static double[] col6Seg=new double[8]; ;
	static double[]  col7Seg=new double[8]; ;
	static byte[]  col8Seg=new byte[8]; ;
	static byte[] col9Seg=new byte[8]; ;
	static int[]  col0MinSeg=new int[8];
	static int[] col1MinSeg =new int[8];
	static int[] col2MinSeg=new int[8]; ;;
	static int[]  col3MinSeg =new int[8];
	static double[] col4MinSeg=new double[8]; 
	static double[]  col5MinSeg =new double[8]; 
	static double[] col6MinSeg =new double[8]; 
	static double[]  col7MinSeg =new double[8]; 
	static byte[]  col8MinSeg=new byte[8];
	static byte[] col9MinSeg=new byte[8];
	static int[]  realcol0Seg=new int[8];
	static int[] realcol1Seg=new int[8]; ;
	static int[] realcol2Seg=new int[8];;
	static int[]  realcol3Seg=new int[8]; ;
	static double[] realcol4Seg=new double[8]; ;
	static double[]  realcol5Seg=new double[8]; ;
	static double[] realcol6Seg=new double[8]; ;
	static double[]  realcol7Seg=new double[8]; ;
	static byte[]  realcol8Seg=new byte[8]; ;
	static byte[] realcol9Seg=new byte[8]; ;
	static int[]  realcol0MinSeg=new int[8];
	static int[] realcol1MinSeg =new int[8];
	static int[] realcol2MinSeg=new int[8]; ;;
	static int[]  realcol3MinSeg =new int[8];
	static double[] realcol4MinSeg=new double[8]; 
	static double[]  realcol5MinSeg =new double[8]; 
	static double[] realcol6MinSeg =new double[8]; 
	static double[]  realcol7MinSeg =new double[8]; 
	static byte[]  realcol8MinSeg=new byte[8];
	static byte[] realcol9MinSeg=new byte[8];
	static  SegmentIndexRef ref ;
	static FSDataInputStream istream;
	public static void main(String[] args) throws IOException {
		String dir = args[0];
		Path pdir = new Path(dir);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		FileStatus[] stat = fs.listStatus(pdir);
		Path[] paths = FileUtil.stat2Paths(stat);
		for (Path fst : paths) {
			try {
				readSegmentFile(fs, fst,args);
				computeMaxAndMin();
				compareMaxAndMin();
				compareSegMaxAndSegMin();
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}

		System.out.println("run   over .............................................");	

	}
	public  static  void computeMaxAndMin() throws IOException{
		int count1=0,count2=0,count3=0,count4=0,count5=0;
		for (int m=0 ;m<ref.numClusters;m++){
			if(m==0){
				int  tmp=0;
				for(int i=0;i<col0Numpairs.length;i++){
					//	if(i==0){
					istream.seek(col0PageOffset[i]);
					System.out.println("col0PageOffset[i]  "+col0PageOffset[i]);
					//}

					//col0[i]=Integer.MAX_VALUE;col0Min[i]=Integer.MIN_VALUE;

					for(int j=0;j<col0Numpairs[i]+3;j++){
						//						for(int  three=0;three<3;three++){
						//							
						//						}
						while(j<3){
							System.out.println(istream.readInt());
							j++;
						}


						tmp=istream.readInt();
						if(j==3){
							col0[i]=tmp;col0Min[i]=tmp;
						}
						//						if(count1<10){
						//							System.out.println("value    "+tmp);
						//							count1++;
						//						}
						if(col0[i]<=tmp){
							col0[i]=tmp ;	
						}
						if(col0Min[i]>=tmp){
							col0Min[i]=tmp ;	
						}
					}
				}
			}
			if(m==1){
				int clusterId=0;
				int  tmp1=0,tmp2=0;
				for(int i=0;i<col1Numpairs.length;i++){
					//	if(i==0){
					istream.seek(col1PageOffset[i]);	
					System.out.println("col1PageOffset[i]  "+col1PageOffset[i]);
					//	}

					col1[i]=Integer.MAX_VALUE;col1Min[i]=Integer.MIN_VALUE;
					col2[i]=Integer.MAX_VALUE;col2Min[i]=Integer.MIN_VALUE;
					for(int j=0;j<col1Numpairs[i]+3;j++){
						while(j<3){
							System.out.println(istream.readInt());
							j++;
						}

						if(clusterId==0){
							tmp1=istream.readInt();
							clusterId++ ;
							if(j==3){
								col1[i]=tmp1;col1Min[i]=tmp1;
							}
							if(col1[i]<tmp1){
								col1[i]=tmp1 ;	
							}
							if(col1Min[i]>=tmp1){
								col1Min[i]=tmp1 ;	
							}
						}
						if(clusterId==1){
							tmp2=istream.readInt();
							if(j==3){
								col2[i]=tmp2;col2Min[i]=tmp2;
							}
							clusterId=0;
							if(count2<10){
								System.out.println("value    "+tmp2);
								count2++;
							}
							if(col2[i]<=tmp2){
								col2[i]=tmp2 ;	
							}
							if(col2Min[i]>=tmp2){
								col2Min[i]=tmp2 ;	
							}
						}



					}
				}	

				//	istream.seek	

			}
			if(m==2){
				//	int clusterId=0;
				int  tmp1=0;
				for(int i=0;i<col2Numpairs.length;i++){
					//	if(i==0){
					istream.seek(col2PageOffset[i]);	
					System.out.println("col2PageOffset[i]  "+col2PageOffset[i]);
					//	}

					col3[i]=Integer.MAX_VALUE;col3Min[i]=Integer.MIN_VALUE;
					//	col2[i]=0;col2Min[i]=0;
					for(int j=0;j<col2Numpairs[i]+3;j++){
						while(j<3){
							System.out.println(istream.readInt());
							j++;
						}

						//    if(clusterId==0){
						tmp1=istream.readInt();
						//	clusterId++ ;
						//    }
						//
						//						if(count3<10){
						//							System.out.println("value    "+tmp1);
						//							count3++;
						//						}

						if(j==3){
							col3[i]=tmp1;col3Min[i]=tmp1;
						}
						if(col3[i]<=tmp1){
							col3[i]=tmp1 ;	
						}
						if(col3Min[i]>=tmp1){
							col3Min[i]=tmp1 ;	
						}

					}	

					//	istream.seek	

				}


			}
			if(m==3){
				int clusterId=0;
				double tmp1=0,tmp2=0,tmp3=0,tmp4;
				for(int i=0;i<col3Numpairs.length;i++){
					//	if(i==0){
					istream.seek(col3PageOffset[i]);
					System.out.println("col3PageOffset[i]  "+col3PageOffset[i]);
					//}

					col4[i]=Double.MAX_VALUE;col4Min[i]=Double.MIN_VALUE;
					col5[i]=Double.MAX_VALUE;col5Min[i]=Double.MIN_VALUE;
					col6[i]=Double.MAX_VALUE;col6Min[i]=Double.MIN_VALUE;
					col7[i]=Double.MAX_VALUE;col7Min[i]=Double.MIN_VALUE;
					for(int j=0;j<col3Numpairs[i]+3;j++){
						while(j<3){
							System.out.println(istream.readInt());
							j++;
						}
						if(clusterId==0){
							tmp1=istream.readDouble();
							clusterId++ ;
							if(j==3){
								col4[i]=tmp1;col4Min[i]=tmp1;
							}
							if(col4[i]<=tmp1){
								col4[i]=tmp1 ;	
							}
							if(col4Min[i]>=tmp1){
								col4Min[i]=tmp1 ;	
							}

							//							if(count4<10){
							//								System.out.println("value    "+tmp1);
							//								count4++;
							//							}
						}
						if(clusterId==1){
							tmp2=istream.readDouble();
							if(j==3){
								col5[i]=tmp2;col5Min[i]=tmp2;
							}
							clusterId++;
							if(col5[i]<=tmp2){
								col5[i]=tmp2 ;	
							}
							if(col5Min[i]>=tmp2){
								col5Min[i]=tmp2 ;	
							}
						}
						if(clusterId==2){
							tmp3=istream.readDouble();
							clusterId++;
							if(j==3){
								col6[i]=tmp3;col6Min[i]=tmp3;
							}
							if(col6[i]<=tmp3){
								col6[i]=tmp3 ;	
							}
							if(col6Min[i]>=tmp3){
								col6Min[i]=tmp3 ;	
							}
						}
						if(clusterId==3){
							tmp4=istream.readDouble();
							clusterId=0;
							if(j==3){
								col7[i]=tmp4;col7Min[i]=tmp4;
							}
							if(col7[i]<=tmp4){
								col7[i]=tmp4 ;	
							}
							if(col7Min[i]>=tmp4){
								col7Min[i]=tmp4 ;	
							}
						}





					}
				}	

				//	istream.seek	

			}

			if(m==4){
				int clusterId=0;
				byte  tmp1=0,tmp2=0;
				for(int i=0;i<col4Numpairs.length;i++){
					//	if(i==0){
					istream.seek(col4PageOffset[i]);	
					System.out.println("col4PageOffset[i]  "+col4PageOffset[i]);
					//	}

					col8[i]=Byte.MAX_VALUE;col8Min[i]=Byte.MIN_VALUE;
					col9[i]=Byte.MAX_VALUE;col9Min[i]=Byte.MIN_VALUE;
					for(int j=0;j<col1Numpairs[i]+3;j++){
						while(j<3){
							System.out.println(istream.readInt());
							j++;
						}
						if(clusterId==0){
							tmp1=istream.readByte();
							clusterId++ ;
							if(j==3){
								col8[i]=tmp1;col8Min[i]=tmp1;
							}
							//							if(count5<10){
							//								System.out.println("value    "+tmp1);
							//								count5++;
							//							}
							if(col8[i]<=tmp1){
								col8[i]=tmp1 ;	
							}
							if(col8Min[i]>=tmp1){
								col8Min[i]=tmp1 ;	
							}
						}
						if(clusterId==1){
							tmp2=istream.readByte();
							clusterId=0;
							if(j==3){
								col9[i]=tmp2;col9Min[i]=tmp2;
							}
							if(col9[i]<=tmp2){
								col9[i]=tmp2 ;	
							}
							if(col9Min[i]>=tmp2){
								col9Min[i]=tmp2 ;	
							}
						}


					}
				}	

				//	istream.seek	

			}

		}

	}

	public static void readSegmentFile(FileSystem fs, Path p,String[] str)
			throws IOException {
		System.out.println(p.toString());
		long fileLength = fs.getFileStatus(p).getLen();

		//	if (fileLength > 0) {
		istream = fs.open(p);
		istream.seek(fileLength - 2 * 4 - 8);

		ref = new SegmentIndexRef();
		ref.numClusters = istream.readInt();
		ref.numSegs = istream.readInt();
		long segIndexOffset = istream.readLong();

		ref.segOffsets = new long[ref.numSegs];
		ref.segPMSOffsets = new long[ref.numSegs];
		ref.segLengths = new long[ref.numSegs];
		ref.segMetas = new PageMeta[ref.numSegs][];
		System.out.println("ref.numClusters   "+ref.numClusters );
		System.out.println("ref.numSegs    "+ref.numSegs  );
		System.out.println("segIndexOffset    "+segIndexOffset );
		istream.seek(segIndexOffset);

		for (int i = 0; i < ref.numSegs; i++) {
			ref.segOffsets[i] = istream.readLong();
			ref.segPMSOffsets[i] = istream.readLong();
			ref.segLengths[i] = istream.readLong();
			ref.segMetas[i] = new PageMeta[ref.numClusters];
			System.out.println("ref.segOffsets[i]    "+ref.segOffsets[i] );		
			System.out.println("ref.segPMSOffsets[i]   "+ref.segPMSOffsets[i] );		

			System.out.println("ref.segLengths[i]  "+ref.segLengths[i]);	

			for (int j = 0; j < ref.numClusters; j++) {
				ref.segMetas[i][j] = new PageMeta();
				ref.segMetas[i][j].readFields(istream);
				PageMeta meta =	ref.segMetas[i][j] ;
				System.out.println("meta.numPairs "+meta.numPairs);
				System.out.println("meta.startPos"+meta.startPos);

				if(j==0){
					//   LOG.info("415 max  :"+Bytes.toInt(pm.max.data, 0)+"min  :"+Bytes.toInt(pm.min.data, 0));
					//System.out.println("i==0 max  :"+Bytes.toInt(meta.max.data, 0)+"min  :"+Bytes.toInt(meta.min.data, 0));

					col0Seg[i]=Bytes.toInt(meta.max.data, 0);
					col0MinSeg[i]=Bytes.toInt(meta.min.data, 0);

				}
				if(j==1){
					col1Seg[i]=Bytes.toInt(meta.max.data, 0);
					col1MinSeg[i]=Bytes.toInt(meta.min.data, 0);
					col2Seg[i]=Bytes.toInt(meta.max.data, 4);
					col2MinSeg[i]=Bytes.toInt(meta.min.data, 4);
				}
				if(j==2){
					col3Seg[i]=Bytes.toInt(meta.max.data, 0);
					col3MinSeg[i]=Bytes.toInt(meta.min.data, 0);

				}
				if(j==3){
					col4Seg[i]=Bytes.toDouble(meta.max.data, 0);
					col4MinSeg[i]=Bytes.toDouble(meta.min.data, 0);
					col5Seg[i]=Bytes.toDouble(meta.max.data, 8);
					col5MinSeg[i]=Bytes.toDouble(meta.min.data, 8);
					col6Seg[i]=Bytes.toDouble(meta.max.data, 16);
					col6MinSeg[i]=Bytes.toDouble(meta.min.data, 16);
					col7Seg[i]=Bytes.toDouble(meta.max.data, 24);
					col7MinSeg[i]=Bytes.toDouble(meta.min.data, 24);
				}
				if(j==4){
					//        LOG.info("i=4 max  :"+Bytes.toLong(pm.max.data, 0)+"min  :"+Bytes.toLong(pm.min.data, 0));
					//	System.out.println("i=4  max  :"+Bytes.toLong(meta.max.data, 0)+"min  :"+Bytes.toLong(meta.min.data, 0));

					col8Seg[i]=meta.max.data[0];
					col8MinSeg[i]=meta.min.data[0];
					col9Seg[i]=meta.max.data[1];
					col9MinSeg[i]=meta.min.data[1];
				}

			}
		}


		//			 segPMSOffsets.add(outputStream.getPos());
		//		      // write the pagemeta section
		//		      LOG.info("And write its page meta section.");
		//		      pms.setPageMetaLists(pageMetaLists);
		//		      pms.write(outputStream);
		//		      // write the cluter index
		//		      LOG.info("And write its clusters' offsets.");
		//		      for (long l : clusterOffsetInCurSegment) {
		//		        outputStream.writeLong(l);
		//		      }
		//
		//		      // record the length of the segment
		//		      long length = outputStream.getPos() - segOffsets.get(curSegIdx);			

		//		      for (long l : clusterOffsetInCurSegment) {
		//	        outputStream.writeLong(l);
		//	      }
		// read  the cluter index
		istream.seek(Long.parseLong(str[1])-9*8);
		//	istream.seek(132981800-9*8);
		//istream.seek( 209206786-9*8);
		//	istream.seek( 209207244-9*8);
		for (int m=0 ;m<ref.numClusters;m++){

			System.out.println("111   clusterOffsetInCurSegment   "+istream.readLong());
		}

		istream.seek(Long.parseLong(str[2]));
		//istream.seek(132959750);
		//istream.seek(209172301);
		//	istream.seek(209194411);
		PageMetaSection pms=new PageMetaSection(true);
		pms.readFields(istream);
		//			int[]  col0;
		//			int[] col1 ;
		//			int[] col2;
		//			int[]  col3 ;
		//			double[] col4 ;
		//			double[]  col5 ;
		//			double[] col6 ;
		//			double[]  col7 ;
		//			byte[]  col8 ;
		//			byte[] col9 ;
		//			int[]  col0Min;
		//			int[] col1Min ;
		//			int[] col2Min;
		//			int[]  col3Min ;
		//			double[] col4Min ;
		//			double[]  col5Min ;
		//			double[] col6Min ;
		//			double[]  col7Min ;
		//			byte[]  col8Min ;
		//			byte[] col9Min ;
		//			int[]  PageMetacol0;
		//			int[] PageMetacol1 ;
		//			int[] PageMetacol2;
		//			int[]  PageMetacol3 ;
		//			double[] PageMetacol4 ;
		//			double[]  PageMetacol5 ;
		//			double[] PageMetacol6 ;
		//			double[]  PageMetacol7 ;
		//			byte[]  PageMetacol8 ;
		//			byte[] PageMetacol9 ;
		//			int[]  PageMetacol0Min;
		//			int[] PageMetacol1Min ;
		//			int[] PageMetacol2Min;
		//			int[]  PageMetacol3Min ;
		//			double[] PageMetacol4Min ;
		//			double[]  PageMetacol5Min ;
		//			double[] PageMetacol6Min ;
		//			double[]  PageMetacol7Min ;
		//			byte[]  PageMetacol8Min ;
		//			byte[] PageMetacol9Min ;
		//			int[]  col0PageOffset,col1PageOffset,col2PageOffset,col3PageOffset,col4PageOffset;
		PageMetaList[] pageMetaList =     pms.getPageMetaLists() ;
		for (int m=0 ;m<ref.numClusters;m++){
			System.out.println("121                  121    ");
			PageMetaList pagemeta=  pageMetaList[m]	;
			List<cn.ac.ncic.mastiff.io.segmentfile.PageMeta> list= pagemeta.getMetaList() ;

			List<Long>   size=pagemeta.getOffsetList();
			switch (m){
			case 0:col0Numpairs=new int[list.size()];col0PageOffset=new long[list.size()];col0=new int[list.size()] ;col0Min=new int[list.size()] ;PageMetacol0=new int[list.size()] ;PageMetacol0Min=new int[list.size()] ;break ;
			case 1: col1Numpairs=new int[list.size()];col1PageOffset=new long[list.size()];;col1=new int[list.size()] ;col1Min=new int[list.size()] ;PageMetacol1=new int[list.size()] ;PageMetacol1Min=new int[list.size()] ;col2=new int[list.size()] ;col2Min=new int[list.size()] ;PageMetacol2=new int[list.size()] ;PageMetacol2Min=new int[list.size()] ;break ;
			case 2: col2Numpairs=new int[list.size()];col2PageOffset=new long[list.size()];col3=new int[list.size()] ;col3Min=new int[list.size()] ;PageMetacol3=new int[list.size()] ;PageMetacol3Min=new int[list.size()] ;break ;
			case 3: col3Numpairs=new int[list.size()];col3PageOffset=new long[list.size()];col4=new double[list.size()] ;col4Min=new double[list.size()] ;PageMetacol4=new double[list.size()] ;PageMetacol4Min=new double[list.size()] ;col5=new double[list.size()] ;col5Min=new double[list.size()] ;PageMetacol5=new double[list.size()] ;PageMetacol5Min=new double[list.size()] ;
			col6=new double[list.size()] ;col6Min=new double[list.size()] ;PageMetacol6=new double[list.size()] ;PageMetacol6Min=new double[list.size()] ;
			col7=new double[list.size()] ;col7Min=new double[list.size()] ;PageMetacol7=new double[list.size()] ;PageMetacol7Min=new double[list.size()] ;break ;
			case 4: col4Numpairs=new int[list.size()];col4PageOffset=new long[list.size()];col8=new byte[list.size()] ;col8Min=new byte[list.size()] ;PageMetacol8=new byte[list.size()] ;PageMetacol8Min=new byte[list.size()] ;
			col9=new byte[list.size()] ;col9Min=new byte[list.size()] ;PageMetacol9=new byte[list.size()] ;PageMetacol9Min=new byte[list.size()] ;break ;
			//				case 5: break ;
			//				case 6: break ;
			//				case 7: break ;
			//				case 8:break ;
			//				case 9:break ;
			default :break ;
			}

			for(int l=0;l<list.size();l++){

				if(m==0){
					//						System.out.println("m==0  page Max Min   "+Bytes.toInt(list.get(l).max.data, 0)+"   min   "+Bytes.toInt(list.get(l).min.data, 0)); 
					//						System.out.println("numpairs  numpairs  numpairs  numpairs  numpairs  "+"pageId  "+l    +"      "+list.get(l).numPairs);
					//						System.out.println("position     position  position  position         "+"pageId  "+l    +"      "+list.get(l).startPos);
					PageMetacol0[l]=Bytes.toInt(list.get(l).max.data, 0);	
					PageMetacol0Min[l]=Bytes.toInt(list.get(l).min.data, 0);
					col0Numpairs[l]=list.get(l).numPairs ;
				}
				if(m==1){
					PageMetacol1[l]=Bytes.toInt(list.get(l).max.data, 0);	
					PageMetacol1Min[l]=Bytes.toInt(list.get(l).min.data, 0);
					PageMetacol2[l]=Bytes.toInt(list.get(l).max.data, 4);	
					PageMetacol2Min[l]=Bytes.toInt(list.get(l).min.data, 4);
					col1Numpairs[l]=list.get(l).numPairs ;
				}
				if(m==2){
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 0)+"   min   "+Bytes.toDouble(list.get(l).min.data, 0)); 
					//
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 8)+"   min   "+Bytes.toDouble(list.get(l).min.data, 8)); 
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 16)+"   min   "+Bytes.toDouble(list.get(l).min.data, 16)); 
					//
					//						System.out.println("m=3    numpairs  numpairs  numpairs  numpairs  numpairs  "+"pageId  "+l    +"      "+list.get(l).numPairs);
					//						System.out.println("m=3  position     position  position  position         "+"pageId  "+l    +"      "+list.get(l).startPos);
					PageMetacol3[l]=Bytes.toInt(list.get(l).max.data, 0);	
					PageMetacol3Min[l]=Bytes.toInt(list.get(l).min.data, 0);
					col2Numpairs[l]=list.get(l).numPairs ;
				}
				if(m==3){
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 0)+"   min   "+Bytes.toDouble(list.get(l).min.data, 0)); 
					//
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 8)+"   min   "+Bytes.toDouble(list.get(l).min.data, 8)); 
					//						System.out.println("m==3  page Max Min   "+"pageId  "+l    +"      "+Bytes.toDouble(list.get(l).max.data, 16)+"   min   "+Bytes.toDouble(list.get(l).min.data, 16)); 
					//
					//						System.out.println("m=3    numpairs  numpairs  numpairs  numpairs  numpairs  "+"pageId  "+l    +"      "+list.get(l).numPairs);
					//						System.out.println("m=3  position     position  position  position         "+"pageId  "+l    +"      "+list.get(l).startPos);
					PageMetacol4[l]=Bytes.toDouble(list.get(l).max.data, 0);	
					PageMetacol4Min[l]=Bytes.toDouble(list.get(l).min.data, 0);
					PageMetacol5[l]=Bytes.toDouble(list.get(l).max.data, 8);	
					PageMetacol5Min[l]=Bytes.toDouble(list.get(l).min.data, 8);
					PageMetacol6[l]=Bytes.toDouble(list.get(l).max.data, 16);	
					PageMetacol6Min[l]=Bytes.toDouble(list.get(l).min.data, 16);
					PageMetacol7[l]=Bytes.toDouble(list.get(l).max.data, 24);	
					PageMetacol7Min[l]=Bytes.toDouble(list.get(l).min.data, 24);
					col3Numpairs[l]=list.get(l).numPairs ;
				}



				if(m==4){
					//						System.out.println("m==4  page Max Min   "+list.get(l).max.data[0]+"   min   "+list.get(l).min.data[0]); 
					//
					//						System.out.println("m==4  page Max Min   "+list.get(l).max.data[1]+"   min   "+list.get(l).min.data[1]); 
					//						// Bytes.toShort(bytes, offset)
					//						//  Bytes.toDouble(bytes, offset)
					//						System.out.println("m=4    numpairs  numpairs  numpairs  numpairs  numpairs  "+list.get(l).numPairs);
					//						System.out.println("m=4   position     position  position  position         "+list.get(l).startPos);

					PageMetacol8[l]=list.get(l).max.data[0];
					PageMetacol8Min[l]=list.get(l).min.data[0];;
					PageMetacol9[l]=list.get(l).max.data[1];
					PageMetacol9Min[l]=list.get(l).min.data[1];
					col4Numpairs[l]=list.get(l).numPairs ;
				}


			}
			//			int[]  col0PageOffset,col1PageOffset,col2PageOffset,col3PageOffset,col4PageOffset;
			//			static int[]  col0Numpairs,col1Numpairs,col2Numpairs,col3Numpairs,col4Numpairs;
			for (int i=0;i<size.size();i++){
				//System.out.println("m=   "+m+"pageIDId  "+i   +"      "+"   pageoffset    "+size.get(i));
				//	int ii=i ;
				switch (m){
				case  0:col0PageOffset[i]=size.get(i);break ;
				case  1:col1PageOffset[i]=size.get(i);break ;
				case  2:col2PageOffset[i]=size.get(i);break ;
				case  3:col3PageOffset[i]=size.get(i);break;
				case  4:col4PageOffset[i]=size.get(i);break ;
				default:  break ;
				}





			}
			//	System.out.println("clusterOffsetInCurSegment   "+istream.readLong());
		}
		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////		
		/////compute   real  Max  and  Min


		////////////////////////////////////////////compare   the Max and  Min
		//					static byte[]  realcol8MinSeg=new byte[2];
		//					static byte[] realcol9MinSeg=new byte[2];		




		//			int  l= 0;
		//			for (long curLength : ref.segLengths) {
		//				System.out.println("Seg " + l + " size: "
		//						+ (curLength / (1024 * 1024)) + "M");
		//				l++;
		//			}
		//			 out.reset();
		//		        out.writeInt(pageLen);
		//		        out.write(page, 0, pageLen);

		//			
		//			istream.seek(134030376);
		//			System.out.println("/////////////////////////read  a  page   ");
		//   int  len=istream.readInt() ;
		//	System.out.println("page length   "+len);
		//			byte[]   bytes=new byte[524288];
		//			istream.read(bytes, 0, 524288) ;
		//			System.out.println("bytes .length  "+bytes.length);
		//		for(int i=0;i<131072;i++){
		//			System.out.println("value   "+istream.readInt());
		//		}
		//	System.out.println("value count  over ");
		//		//	istream.readInt();
		////			ValPair  max= new ValPair() ;
		////			max.readFields(istream);
		////			//ValPair  max= new ValPair().readFields(istream);
		////			ValPair  min= new ValPair() ;
		////			min.readFields(istream);
		//////			 out.writeInt(pms[i].startPos);
		//////		        out.writeInt(pms[i].numPairs);
		////			System.out.println("startPost            "+istream.readInt());
		////			System.out.println("numPairs                "+istream.readInt());
		////			System.out.println(max.length);
		////			System.out.println(max.offset);
		////			System.out.println(max.pos);
		////			System.out.println(min.length);
		////			System.out.println(min.offset);
		////			System.out.println(min.pos);
		////			
		////			
		//			
		//			
		//		//	istream.seek(209172301)  ;
		//			
		//			istream.seek(50855624);
		//istream.seek(162354964) ;


		//	System.out.println("/////////////////////////read  a  page   ");
		//		System.out.println("bytes .length  "+bytes.length);
		//	int  tmp =0 ;
		//	for(int i=0;i<131072*2;i++){
		//		System.out.println("value   "+istream.readByte());
		//	tmp++ ;
		//	}
		//System.out.println("tmp   "+tmp);		
		//   int  mlen=istream.readInt() ;
		//		//	System.out.println("mpage length   "+mlen);
		//			byte[]   mbytes=new byte[524288];
		//			istream.read(mbytes,0, 524288) ;
		//			System.out.println("mbytes .length  "+mbytes.length);
		//			ValPair  mmax= new ValPair() ;
		//			mmax.readFields(istream);
		//			
		//			//ValPair  max= new ValPair().readFields(istream);
		//			ValPair  mmin= new ValPair() ;
		//			mmin.readFields(istream);
		////			 out.writeInt(pms[i].startPos);
		////		        out.writeInt(pms[i].numPairs);
		//			System.out.println(mmax.length);
		//			System.out.println(mmax.offset);
		//			System.out.println(mmax.pos);
		//			System.out.println(mmin.length);
		//			System.out.println(mmin.offset);
		//			System.out.println(mmin.pos);
		//			System.out.println("startPost            "+istream.readInt());
		//			System.out.println("numPairs                "+istream.readInt());
		//				
		//			
		//	}

	}
	public static  void compareSegMaxAndSegMin(){

		//		for (int n=0 ;n<ref.numClusters;n++){
		//			if(n==0){
		//				if(realcol0MinSeg[0]!=col0MinSeg[0]){
		//					System.out.println("realcol0MinSeg[0]!=col0MinSeg[0]   "+realcol0MinSeg[0]+"     "+col0MinSeg[0]);
		//				}
		//				System.out.println("col0MinSeg[0]   "+col0MinSeg[0]);
		//				System.out.println("col0Seg[0]   "+col0Seg[0]);
		//				if(realcol0Seg[0]!=col0Seg[0]){
		//					System.out.println("realcol0Seg[0]!=col0Seg[0]   "+realcol0Seg[0]+"     "+col0Seg[0]);
		//				}
		//			
		//
		//			}
		//			if(n==1){
		//				if(realcol1MinSeg[0]!=col1MinSeg[0]){
		//					System.out.println("realcol1MinSeg[0]!=col1MinSeg[0]   "+realcol1MinSeg[0]+"     "+col1MinSeg[0]);
		//				}
		//				if(realcol1Seg[0]!=col1Seg[0]){
		//					System.out.println("realcol1nSeg[0]!=col1Seg[0]   "+realcol1Seg[0]+"     "+col1Seg[0]);
		//				}
		//				if(realcol2MinSeg[0]!=col2MinSeg[0]){
		//					System.out.println("realcol2MinSeg[0]!=col21MinSeg[0]   "+realcol2MinSeg[0]+"     "+col2MinSeg[0]);
		//				}
		//				if(realcol2Seg[0]!=col2Seg[0]){
		//					System.out.println("realcol2nSeg[0]!=col2Seg[0]   "+realcol2Seg[0]+"     "+col2Seg[0]);
		//				}
		//				System.out.println("col1MinSeg[0]   "+col1MinSeg[0]);
		//				System.out.println("col1Seg[0]   "+col1Seg[0]);
		//				System.out.println("col2MinSeg[0]   "+col2MinSeg[0]);
		//				System.out.println("col2Seg[0]   "+col2Seg[0]);
		//			}
		//			if(n==2){
		//				if(realcol3MinSeg[0]!=col3MinSeg[0]){
		//					System.out.println("realcol3MinSeg[0]!=col3MinSeg[0]   "+realcol3MinSeg[0]+"     "+col3MinSeg[0]);
		//				}
		//				if(realcol3Seg[0]!=col3Seg[0]){
		//					System.out.println("realcol3nSeg[0]!=col3Seg[0]   "+realcol3Seg[0]+"     "+col3Seg[0]);
		//				}
		//				
		//				
		//				System.out.println("col3MinSeg[0]   "+col3MinSeg[0]);
		//				System.out.println("col3Seg[0]   "+col3Seg[0]);
		//			}
		//			if(n==3){
		//				if(realcol4MinSeg[0]!=col4MinSeg[0]){
		//					System.out.println("realcol4MinSeg[0]!=col4MinSeg[0]   "+realcol4MinSeg[0]+"     "+col4MinSeg[0]);
		//				}
		//				if(realcol4Seg[0]!=col4Seg[0]){
		//					System.out.println("realcol4nSeg[0]!=col4Seg[0]   "+realcol4Seg[0]+"     "+col4Seg[0]);
		//				}
		//
		//				if(realcol5MinSeg[0]!=col5MinSeg[0]){
		//					System.out.println("realcol5MinSeg[0]!=col5MinSeg[0]   "+realcol5MinSeg[0]+"     "+col5MinSeg[0]);
		//				}
		//				if(realcol5Seg[0]!=col5Seg[0]){
		//					System.out.println("realcol5nSeg[0]!=col5Seg[0]   "+realcol5Seg[0]+"     "+col5Seg[0]);
		//				}
		//
		//				if(realcol6MinSeg[0]!=col6MinSeg[0]){
		//					System.out.println("realcol6MinSeg[0]!=col6MinSeg[0]   "+realcol6MinSeg[0]+"     "+col6MinSeg[0]);
		//				}
		//				if(realcol6Seg[0]!=col6Seg[0]){
		//					System.out.println("realcol6nSeg[0]!=col6Seg[0]   "+realcol6Seg[0]+"     "+col6Seg[0]);
		//				}
		//
		//				if(realcol7MinSeg[0]!=col7MinSeg[0]){
		//					System.out.println("realcol7MinSeg[0]!=col7MinSeg[0]   "+realcol7MinSeg[0]+"     "+col7MinSeg[0]);
		//				}
		//				if(realcol7Seg[0]!=col7Seg[0]){
		//					System.out.println("realcol7nSeg[0]!=col7Seg[0]   "+realcol7Seg[0]+"     "+col7Seg[0]);
		//				}
		//				
		//				System.out.println("col4MinSeg[0]   "+col4MinSeg[0]);
		//				System.out.println("col40Seg[0]   "+col4Seg[0]);
		//				System.out.println("col5MinSeg[0]   "+col5MinSeg[0]);
		//				System.out.println("col5Seg[0]   "+col5Seg[0]);
		//				System.out.println("col6MinSeg[0]   "+col6MinSeg[0]);
		//				System.out.println("col6Seg[0]   "+col6Seg[0]);
		//				System.out.println("col7MinSeg[0]   "+col7MinSeg[0]);
		//				System.out.println("col7Seg[0]   "+col7Seg[0]);
		//			}
		//
		//			if(n==4){
		//				if(realcol8MinSeg[0]!=col8MinSeg[0]){
		//					System.out.println("realcol8MinSeg[0]!=col8MinSeg[0]   "+realcol8MinSeg[0]+"     "+col8MinSeg[0]);
		//				}
		//				if(realcol8Seg[0]!=col8Seg[0]){
		//					System.out.println("realcol8nSeg[0]!=col8Seg[0]   "+realcol8Seg[0]+"     "+col8Seg[0]);
		//				}
		//
		//				if(realcol9MinSeg[0]!=col9MinSeg[0]){
		//					System.out.println("realcol9MinSeg[0]!=col9MinSeg[0]   "+realcol9MinSeg[0]+"     "+col9MinSeg[0]);
		//				}
		//				if(realcol9Seg[0]!=col9Seg[0]){
		//					System.out.println("realcol9nSeg[0]!=col9Seg[0]   "+realcol9Seg[0]+"     "+col9Seg[0]);
		//				}
		//				
		//				
		//				
		//			}
		//		}
		for (int n=0 ;n<ref.numClusters;n++){
			if(n==0){
				if(realcol0MinSeg[0]!=col0MinSeg[1]){
					System.out.println("realcol0MinSeg[0]!=col0MinSeg[0]   "+realcol0MinSeg[0]+"     "+col0MinSeg[1]);
				}
				System.out.println("col0MinSeg[0]   "+col0MinSeg[1]);
				System.out.println("col0Seg[0]   "+col0Seg[1]);
				if(realcol0Seg[0]!=col0Seg[1]){
					System.out.println("realcol0Seg[0]!=col0Seg[0]   "+realcol0Seg[0]+"     "+col0Seg[1]);
				}


			}
			if(n==1){
				if(realcol1MinSeg[0]!=col1MinSeg[1]){
					System.out.println("realcol1MinSeg[0]!=col1MinSeg[0]   "+realcol1MinSeg[0]+"     "+col1MinSeg[1]);
				}
				if(realcol1Seg[0]!=col1Seg[1]){
					System.out.println("realcol1nSeg[0]!=col1Seg[0]   "+realcol1Seg[0]+"     "+col1Seg[1]);
				}
				if(realcol2MinSeg[0]!=col2MinSeg[1]){
					System.out.println("realcol2MinSeg[0]!=col21MinSeg[0]   "+realcol2MinSeg[0]+"     "+col2MinSeg[1]);
				}
				if(realcol2Seg[0]!=col2Seg[1]){
					System.out.println("realcol2nSeg[0]!=col2Seg[0]   "+realcol2Seg[0]+"     "+col2Seg[1]);
				}
				System.out.println("col1MinSeg[0]   "+col1MinSeg[1]);
				System.out.println("col1Seg[0]   "+col1Seg[1]);
				System.out.println("col2MinSeg[0]   "+col2MinSeg[1]);
				System.out.println("col2Seg[0]   "+col2Seg[1]);
			}
			if(n==2){
				if(realcol3MinSeg[0]!=col3MinSeg[1]){
					System.out.println("realcol3MinSeg[0]!=col3MinSeg[0]   "+realcol3MinSeg[0]+"     "+col3MinSeg[1]);
				}
				if(realcol3Seg[0]!=col3Seg[1]){
					System.out.println("realcol3nSeg[0]!=col3Seg[0]   "+realcol3Seg[0]+"     "+col3Seg[1]);
				}


				System.out.println("col3MinSeg[0]   "+col3MinSeg[1]);
				System.out.println("col3Seg[0]   "+col3Seg[1]);
			}
			if(n==3){
				if(realcol4MinSeg[0]!=col4MinSeg[1]){
					System.out.println("realcol4MinSeg[0]!=col4MinSeg[0]   "+realcol4MinSeg[0]+"     "+col4MinSeg[1]);
				}
				if(realcol4Seg[0]!=col4Seg[1]){
					System.out.println("realcol4nSeg[0]!=col4Seg[0]   "+realcol4Seg[0]+"     "+col4Seg[1]);
				}

				if(realcol5MinSeg[0]!=col5MinSeg[1]){
					System.out.println("realcol5MinSeg[0]!=col5MinSeg[0]   "+realcol5MinSeg[0]+"     "+col5MinSeg[1]);
				}
				if(realcol5Seg[0]!=col5Seg[1]){
					System.out.println("realcol5nSeg[0]!=col5Seg[0]   "+realcol5Seg[0]+"     "+col5Seg[1]);
				}

				if(realcol6MinSeg[0]!=col6MinSeg[1]){
					System.out.println("realcol6MinSeg[0]!=col6MinSeg[0]   "+realcol6MinSeg[0]+"     "+col6MinSeg[1]);
				}
				if(realcol6Seg[0]!=col6Seg[1]){
					System.out.println("realcol6nSeg[0]!=col6Seg[0]   "+realcol6Seg[0]+"     "+col6Seg[1]);
				}

				if(realcol7MinSeg[0]!=col7MinSeg[1]){
					System.out.println("realcol7MinSeg[0]!=col7MinSeg[0]   "+realcol7MinSeg[0]+"     "+col7MinSeg[1]);
				}
				if(realcol7Seg[0]!=col7Seg[1]){
					System.out.println("realcol7nSeg[0]!=col7Seg[0]   "+realcol7Seg[0]+"     "+col7Seg[1]);
				}

				System.out.println("col4MinSeg[0]   "+col4MinSeg[1]);
				System.out.println("col40Seg[0]   "+col4Seg[1]);
				System.out.println("col5MinSeg[0]   "+col5MinSeg[1]);
				System.out.println("col5Seg[0]   "+col5Seg[1]);
				System.out.println("col6MinSeg[0]   "+col6MinSeg[1]);
				System.out.println("col6Seg[0]   "+col6Seg[1]);
				System.out.println("col7MinSeg[0]   "+col7MinSeg[1]);
				System.out.println("col7Seg[0]   "+col7Seg[1]);
			}

			if(n==4){
				if(realcol8MinSeg[0]!=col8MinSeg[1]){
					System.out.println("realcol8MinSeg[0]!=col8MinSeg[0]   "+realcol8MinSeg[0]+"     "+col8MinSeg[1]);
				}
				if(realcol8Seg[0]!=col8Seg[1]){
					System.out.println("realcol8nSeg[0]!=col8Seg[0]   "+realcol8Seg[0]+"     "+col8Seg[1]);
				}

				if(realcol9MinSeg[0]!=col9MinSeg[1]){
					System.out.println("realcol9MinSeg[0]!=col9MinSeg[0]   "+realcol9MinSeg[0]+"     "+col9MinSeg[1]);
				}
				if(realcol9Seg[0]!=col9Seg[1]){
					System.out.println("realcol9nSeg[0]!=col9Seg[0]   "+realcol9Seg[0]+"     "+col9Seg[1]);
				}



			}
		}
	}
	public static  void  compareMaxAndMin(){
		for (int n=0 ;n<ref.numClusters;n++){
			if(n==0){

				for(int i=0;i<col0.length;i++){

					if(i==0){
						realcol0Seg[0]=col0[i];
						realcol0MinSeg[0]=col0Min[i];
					}
					if(realcol0Seg[0]<=col0[i]){
						realcol0Seg[0]=col0[i];
					}
					if(realcol0MinSeg[0]>=col0Min[i]){
						realcol0MinSeg[0]=col0Min[i];
					}

					if(col0[i]!=PageMetacol0[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col0[i]=  "+col0[i]+"    PageMetacol0[i]  "+PageMetacol0[i]);
					}
					if(col0Min[i]!=PageMetacol0Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"   col0Min[i]=  "+col0Min[i]+"    PageMetacol0Min[i]  "+PageMetacol0Min[i]);
					}

				}

			}
			if(n==1){

				for(int i=0;i<col1.length;i++){
					if(i==0){
						realcol1Seg[0]=col1[i];
						realcol1MinSeg[0]=col1Min[i];
					}

					if(realcol1Seg[0]<=col1[i]){
						realcol1Seg[0]=col1[i];
					}
					if(realcol1MinSeg[0]>=col1Min[i]){
						realcol1MinSeg[0]=col1Min[i];
					}

					if(col1[i]!=PageMetacol1[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col1[i]=  "+col1[i]+"    PageMetacol1[i]  "+PageMetacol1[i]);
					}
					if(col1Min[i]!=PageMetacol1Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col1Min[i]=  "+col1Min[i]+"    PageMetacol1Min[i]  "+PageMetacol1Min[i]);
					}

				}
				for(int i=0;i<col2.length;i++){
					if(i==0){
						realcol2Seg[0]=col2[i];
						realcol2MinSeg[0]=col2Min[i];
					}

					if(realcol2Seg[0]<col2[i]){
						realcol2Seg[0]=col2[i];
					}
					if(realcol2MinSeg[0]>=col2Min[i]){
						realcol2MinSeg[0]=col2Min[i];
					}

					if(col2[i]!=PageMetacol2[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col2[i]=  "+col2[i]+"    PageMetacol2[i]  "+PageMetacol2[i]);
					}
					if(col2Min[i]!=PageMetacol2Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col2Min[i]=  "+col2Min[i]+"    PageMetacol2Min[i]  "+PageMetacol2Min[i]);
					}

				}	
			}
			if(n==2){

				for(int i=0;i<col3.length;i++){

					if(i==0){
						realcol3Seg[0]=col3[i];
						realcol3MinSeg[0]=col3Min[i];
					}


					if(realcol3Seg[0]<=col3[i]){
						realcol3Seg[0]=col3[i];
					}
					if(realcol3MinSeg[0]>=col3Min[i]){
						realcol3MinSeg[0]=col3Min[i];
					}

					if(col3[i]!=PageMetacol3[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col3[i]=  "+col3[i]+"    PageMetacol3[i]  "+PageMetacol3[i]);
					}
					if(col3Min[i]!=PageMetacol3Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col3Min[i]=  "+col3Min[i]+"    PageMetacol3Min[i]  "+PageMetacol3Min[i]);
					}

				}

			}


			if(n==3){

				for(int i=0;i<col4.length;i++){
					if(i==0){
						realcol4Seg[0]=col4[i];
						realcol4MinSeg[0]=col4Min[i];
					}


					if(realcol4Seg[0]<=col4[i]){
						realcol4Seg[0]=col4[i];
					}
					if(realcol4MinSeg[0]>=col4Min[i]){
						realcol4MinSeg[0]=col4Min[i];
					}

					if(col4[i]!=PageMetacol4[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"   col4[i]=  "+col4[i]+"    PageMetacol4[i]  "+PageMetacol4[i]);
					}
					if(col4Min[i]!=PageMetacol4Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"   col4Min[i]=  "+col4Min[i]+"    PageMetacol4Min[i]  "+PageMetacol4Min[i]);
					}

				}
				for(int i=0;i<col5.length;i++){
					if(i==0){
						realcol5Seg[0]=col5[i];
						realcol5MinSeg[0]=col5Min[i];
					}


					if(realcol5Seg[0]<=col5[i]){
						realcol5Seg[0]=col5[i];
					}
					if(realcol5MinSeg[0]>=col5Min[i]){
						realcol5MinSeg[0]=col5Min[i];
					}

					if(col5[i]!=PageMetacol5[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col5[i]=  "+col5[i]+"    PageMetacol5[i]  "+PageMetacol5[i]);
					}
					if(col5Min[i]!=PageMetacol5Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col5Min[i]=  "+col5Min[i]+"    PageMetacol5Min[i]  "+PageMetacol5Min[i]);
					}

				}	

				for(int i=0;i<col6.length;i++){
					if(i==0){
						realcol6Seg[0]=col6[i];
						realcol6MinSeg[0]=col6Min[i];
					}



					if(realcol6Seg[0]<=col6[i]){
						realcol6Seg[0]=col6[i];
					}
					if(realcol6MinSeg[0]>=col6Min[i]){
						realcol6MinSeg[0]=col6Min[i];
					}

					if(col6[i]!=PageMetacol6[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"    col6[i]=  "+col6[i]+"    PageMetacol6[i]  "+PageMetacol6[i]);
					}
					if(col6Min[i]!=PageMetacol6Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col6Min[i]=  "+col6Min[i]+"    PageMetacol6Min[i]  "+PageMetacol6Min[i]);
					}

				}

				for(int i=0;i<col7.length;i++){
					if(i==0){
						realcol7Seg[0]=col7[i];
						realcol7MinSeg[0]=col7Min[i];
					}


					if(realcol7Seg[0]<=col7[i]){
						realcol7Seg[0]=col7[i];
					}
					if(realcol7MinSeg[0]>=col7Min[i]){
						realcol7MinSeg[0]=col7Min[i];
					}

					if(col7[i]!=PageMetacol7[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col7[i]=  "+col7[i]+"    PageMetacol7[i]  "+PageMetacol7[i]);
					}
					if(col7Min[i]!=PageMetacol7Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col7Min[i]=  "+col7Min[i]+"    PageMetacol7Min[i]  "+PageMetacol7Min[i]);
					}

				}
			}

			if(n==4){
				for(int i=0;i<col8.length;i++){

					if(i==0){
						realcol8Seg[0]=col8[i];
						realcol8MinSeg[0]=col8Min[i];
					}

					if(realcol8Seg[0]<=col8[i]){
						realcol8Seg[0]=col8[i];
					}
					if(realcol8MinSeg[0]>=col8Min[i]){
						realcol8MinSeg[0]=col8Min[i];
					}

					if(col8[i]!=PageMetacol8[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col8[i]=  "+col8[i]+"    PageMetacol8[i]  "+PageMetacol8[i]);
					}
					if(col8Min[i]!=PageMetacol8Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col8Min[i]=  "+col8Min[i]+"    PageMetacol8Min[i]  "+PageMetacol8Min[i]);
					}

				}	


				for(int i=0;i<col9.length;i++){
					if(i==0){
						realcol9Seg[0]=col9[i];
						realcol9MinSeg[0]=col9Min[i];
					}

					if(realcol9Seg[0]<=col9[i]){
						realcol9Seg[0]=col9[i];
					}
					if(realcol9MinSeg[0]>=col9Min[i]){
						realcol9MinSeg[0]=col9Min[i];
					}

					if(col9[i]!=PageMetacol9[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col9[i]=  "+col9[i]+"    PageMetacol9[i]  "+PageMetacol9[i]);
					}
					if(col9Min[i]!=PageMetacol9Min[i]){
						System.out.println("Wrong   "+"clusterId=  "+n  +"    PageId  i=  "+i+"     col9Min[i]=  "+col9Min[i]+"    PageMetacol9Min[i]  "+PageMetacol9Min[i]);
					}

				}	
			}

		}			


	}
	public static class SegmentIndexRef {
		public int numSegs;
		public int numClusters;
		public long[] segOffsets;
		public long[] segPMSOffsets;
		public long[] segLengths;
		public PageMeta[][] segMetas;
	}

	public static class PageMeta {
		public ValPair max = null;
		public ValPair min = null;

		public int startPos;
		public int numPairs;

		public void readFields(DataInput in) throws IOException {
			max = new ValPair();
			min = new ValPair();
			max.readFields(in);
			min.readFields(in);
			startPos = in.readInt();
			numPairs = in.readInt();
		}
	}

	public static class ValPair {
		/** Reference to the raw data bytes */
		public byte[] data;
		/** Offset of the raw data bytes in a byte array */
		public int offset;
		/** Length of the raw data bytes */
		public int length;

		/** Position in a specified segment */
		public int pos;

		public void readFields(DataInput in) throws IOException {
			length = in.readInt();
			data = new byte[length];
			offset = 0;
			in.readFully(data);
		}
	}
}
