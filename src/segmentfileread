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
	public static void main(String[] args) throws IOException {
		String dir = args[0];
		Path pdir = new Path(dir);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dir), conf);
		FileStatus[] stat = fs.listStatus(pdir);
		Path[] paths = FileUtil.stat2Paths(stat);
		for (Path fst : paths) {
			try {
				readSegmentFile(fs, fst);
			} catch (IOException e) {
				// TODO Auto-generated catch block
			}
		}

		System.out.println("run   over .............................................");	

	}

	public static void readSegmentFile(FileSystem fs, Path p)
			throws IOException {
		System.out.println(p.toString());
		long fileLength = fs.getFileStatus(p).getLen();

		if (fileLength > 0) {
			FSDataInputStream istream = fs.open(p);
			istream.seek(fileLength - 2 * 4 - 8);

			SegmentIndexRef ref = new SegmentIndexRef();
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
						System.out.println("i==0 max  :"+Bytes.toInt(meta.max.data, 0)+"min  :"+Bytes.toInt(meta.min.data, 0));
					}
					if(j==4){
						//        LOG.info("i=4 max  :"+Bytes.toLong(pm.max.data, 0)+"min  :"+Bytes.toLong(pm.min.data, 0));
						System.out.println("i=4  max  :"+Bytes.toLong(meta.max.data, 0)+"min  :"+Bytes.toLong(meta.min.data, 0));
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

			//	istream.seek(132981800-9*8);
			//	istream.seek( 209206786-9*8);
			istream.seek( 209207244-9*8);
			for (int m=0 ;m<ref.numClusters;m++){

				System.out.println("111   clusterOffsetInCurSegment   "+istream.readLong());
			}


			//istream.seek(132959750);
			//istream.seek(209172301);
			istream.seek(209194411);
			PageMetaSection pms=new PageMetaSection(true);
			pms.readFields(istream);

			PageMetaList[] pageMetaList =     pms.getPageMetaLists() ;
			for (int m=0 ;m<ref.numClusters;m++){
				System.out.println("121                  121    ");
				PageMetaList pagemeta=  pageMetaList[m]	;
				List<cn.ac.ncic.mastiff.io.segmentfile.PageMeta> list= pagemeta.getMetaList() ;

				List<Long>   size=pagemeta.getOffsetList();
				for(int l=0;l<list.size();l++){

					if(m==0){
						System.out.println("m==0  page Max Min   "+Bytes.toInt(list.get(l).max.data, 0)+"   min   "+Bytes.toInt(list.get(l).min.data, 0)); 
						System.out.println("numpairs  numpairs  numpairs  numpairs  numpairs  "+list.get(l).numPairs);
						System.out.println("position     position  position  position         "+list.get(l).startPos);

					}
					if(m==1){
						System.out.println("m==1  page Max Min   "+Bytes.toInt(list.get(l).max.data, 0)+"   min   "+Bytes.toInt(list.get(l).min.data, 0)); 

						System.out.println("m==1  page Max Min   "+Bytes.toInt(list.get(l).max.data, 4)+"   min   "+Bytes.toInt(list.get(l).min.data, 4)); 

						System.out.println("m=1    numpairs  numpairs  numpairs  numpairs  numpairs  "+list.get(l).numPairs);
						System.out.println("m=1   position     position  position  position         "+list.get(l).startPos);
					}
					if(m==8){
						System.out.println("m==1  page Max Min   "+Bytes.toInt(list.get(l).max.data, 0)+"   min   "+Bytes.toInt(list.get(l).min.data, 0)); 

						System.out.println("m==1  page Max Min   "+Bytes.toInt(list.get(l).max.data, 4)+"   min   "+Bytes.toInt(list.get(l).min.data, 4)); 

						System.out.println("m=1    numpairs  numpairs  numpairs  numpairs  numpairs  "+list.get(l).numPairs);
						System.out.println("m=1   position     position  position  position         "+list.get(l).startPos);
					}


				}

				for (int i=0;i<size.size();i++){
					System.out.println("m=   "+m+"   pageoffset    "+size.get(i));
				}
				//	System.out.println("clusterOffsetInCurSegment   "+istream.readLong());
			}
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
