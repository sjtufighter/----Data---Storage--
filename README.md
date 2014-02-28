Flexible-optimized  segment  file(FOSF)

   FOSF  is  a  new storage format  which  is  more efficient ,flexible . It  can  also  save  more storage spaces  and speed up the sql  query than Hive-RCFile. It  has    similarity   with  ORC File  on  both  use the IndexFilter to speed  up  the query . http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.0.2/ds_Hive/orcfile.html.

   FOSF  is adapted from the previouse file format mastiff-segmentfile , which  does  not have relationship whith Hive . Mastiff  is  a  time-based system which  is used for data stream proccess .We find its storage format-- segmentFile  has  a  theoretic  advantage -- MaxMinIndex  which can be used for  the where--filter  in  Sql of  Hive. But  it has  some defaults: this mastiff's  pratical efficieny  can  not face up  with  its theory advantages； mastiff  has  the  design  errors  in  its  IndexFilterComputing ;mastiff  is  a  small minority system  which lack of  more   engineers  to develop  ，optimize   and  extended   especially    when  compared with Apache Hive team ；at last ,this  system lack  of  efficient , flexible 
encoding methodhs and  just use the heavy compression  LZO and ZLIB .

   
    At  the same  time, I  focused  on  the Parquet   
https://github.com/Parquet  ,which   has  efficient encoding methods for different Types   and I  also  pay attention  to the ORC ,which have a little  encoding methods .


SO ,WHAT  IS  THIS PROJECT FOR ?  

   I have addapted the segmentFile design and then implement  this format  in Hive to support a  new fast storage format which  is faster than RCFile ,even  faster than ORC.  Build on  this format ,I  try  to extract   the basic  encoding ways  in Parquet and ORC ,such as RLE,bit packing ,Delta encoding ，ZigZar，Dictionary,VLQ  and google Proto buffer. This is the first step ,then I  make  up   this basic encoding ways  to  synthetic  data encoding is more  efficient.    At last I implement this flexible encoding   in  HIve-FOSF  format  .

Thanks



CODES  OF CONDUCT  AND  HOW  TO  RUN

   this sorce code is combined with  two  protions: optimited segmentFile  and flexible Encoding.  In  order  to  run  this fast  storage format FOSf  on   your  cluster ,you should  combine this FOSf   with Hive ,We  should  use Hive's  Quey interpreter    and lauch  the  mapreduce  job . I just provide a  storage format   implemented by  Hive -storageHandle  mechanism.  And  this format  will  need  some  third-party jars  such  as  mastiff.jar ,  google  ProtoBuffer.jar,Snappy.jar , fastutil.jar  etc....In  should  be  noted  that  FOSF‘s  metadata   is  storaged   in a linghtweight database other than the Hive's derby database ，this  database  is realized  by  my partener.

Environment  Configuration

   
    this storage is   realized   by   Java  and  build on Hadoop . So JDK  above 1.6   is needed . We  apply ant to  bulid  this  projetc.

CONVEY  THANKS

   
    thanks JieYiShen  for  his  help   in    instruct  me ! 
    
This  is  my first open source project at  here , it  is  my  pleasure to share with you ! Many  Thanks
