
flexible-optimized  segment  file(FOSF)
FOSF  is  a  new storage format  which  is  more efficient ,flexible and  can  save  more storage space  and speed up the sql  query than HIve-RCFile。 It  have  some  imilarity  with  ORC File.

FOSF  is adapted from the previouse file format -mastiff-segmentfile.Mastiff  is  a  time-based system which  is used for data stream proccess .We find it's storage format segmentFile  have  some   theoretic  advantages such as MaxMinIndex which can be used for  where filter  in  Sql of  Hive. But  it has  some defaults: this mastiff system's  pratical efficieny  can  not face up  with  its theory advantages； mastiff  has  the  design  errors  in  its  IndexFilterComputing ;mastiff  is  a  small minority system  which also is lack of  more   engineers  to develop  ，optimize   and  extended   especially    when  compared with Apache Hive；this  system lack  of  efficient  flexible 
encoding methodhs and  just use the heavy compression  LZO and ZLIB .
 At  the same  time, I  focus on  the Parquet   
https://github.com/Parquet  ,which   has  efficient encoding methods for different Types   and I  also  pay attention  to the ORC ,which have a little  encoding methods .


So ,what  is  my  project do?  

I have addapted the segmentFile disign and then implement  this format  in Hive to support a  new fast storage format which  is more fast than RCFile ,even than ORC.At  the same format ,I  trt to extract   the basci encoding methods in Parquet and ORC ,such as RLE,bit packing ,Delta encoding ，ZigZar，Dictionary,VLQ  and google Proto buffer This is the first step ,then I try to combined this basic encoding  to  make data encoding is more  efficient.    At last I implement this flexible encoding   in  HIve-FOSF  format  .

Thanks
