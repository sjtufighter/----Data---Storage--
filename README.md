  这个项目主要目的是对Hive所支持的存储结构进行扩展。当前Hive所支持的存储结构是textfile,sequencefile,rcfile以及最新出来的ORC。他们各自的优势在这里就不详细说明了。下面谈谈要实现的存储结构FOSF（Flexible Optimized SegmentFile)的特点。
 
FOSF存储结构
=========
   FOSF结合了行和列混合存储的方式，行存储主要是用来保证一个元组的所有属性都存储在同一个block上，这样可以大大避免查询时期元组重构所带来的跨节点的网络IO开销，而采用列存储的话，一则是对于海量数据的OLAP这种查询而言可以避免查询不相关的列加载进入内存，二是由于数据列都是同一种数据类型，这使得列存储的压缩性能非常好。需要说明的是，先前曾尝试着采用列簇存储的方式，当初的动机是让查询相关的列放在一个列簇里面，这样的话可以减少查询时候的磁盘IO开销，但是这种方式就是灵活度太差，而且对于查询涉及到多个列簇的话等情况会加载额外的数据列进入内存，而且列簇的加载和重组也是一笔不小的开销，得不偿失。鉴于此，后来就放弃了列簇的想法。
  
   接下来，在行和列混合存储的基础上，引入了索引机制。就是为每一个列设立一个最小基本单元，基本单元可以byte size为单位，也可以记录个数为单位建立粗糙的索引，即记录这个列的最大值和最小值。请注意这是一种粗糙索引。这样的话在sql查询中会起到一定的过滤作用，可以避免查询涉及的列中无关的一部分数据加载进入内存。在当前的Hive执行流程中，如果需要利用此Index进行过滤的话，需要实现 filterPushDown机制。
    
     
   紧接着，鉴于当前所采用的压缩算法大多是LZO ZLIB等重量级的压缩编码方式，为了提高压缩性能进而节省存储空间以及提高查询性能，我们决定依据每一个数据列的属性和分布情况（连续重复，重复不连续，等差数列，相邻数值增量较小，甚至非重复个数较少等）采用最适合其的编码方式，具体的编码方式有deltabitPacking, DictionaryRedBlackTree,RunLengthEncoding,googleProtoBufferVLQ等编码，核心是考虑数据类型特点以及分布情况，关键技术是位编码，增量编码，字典编码，VLQ等,编码部分参考了twitter开源的产品 ，但是其算法是和其程序耦合在一起的，耦合度较高，需要开发者自己花功夫从其中提取，并在现有编码基础上组合出更加高效的编码方式。
  
实现方式
----
   那这个存储结构是如何实现到Hive里面的呢？Hive提供了一个名为storagehandel的接口用于扩展其存储结构。用户只需要实现其相关接口，便可以让其支持你的存储结构。请注意，是支持基本功能，后来发现选择storageHandel来扩展存储结构有点坑。
FOSF性能如何？
----
   在实现了上述功能之后，做了一个较为详细的性能测试，不管是从编码时间，解码时间，压缩比 以及sql查询时间来看都有较为明显的性能提升，github好像上不了PDF，就不方便和大家分享了。
FOSF当前存在的问题以及后期规划：
-
   1.由于采用storageHandel接口来扩展FOSF，这就导致了很多问题，第一不支持小文件合并，这就直接导致我们在小文件较小的时候，map任务数过多而导致大大增加任务启动时间和查询时间。所以只能开发者实现小文件合并，其他 还有诸多查询方式居然也不能支持比如count(1)，只能开发者自己改，表示头都大了。

   2.耦合性和sqllite考虑到Apache  Hive社区版本更新很快，为了能让Hive支持FOSF扩展结构，尽量采用松耦合的方式，所以绝大多数代码（约20000行）全部是在存储结构自己的逻辑中实现中，涉及到修改Hive源码的地方比较少，榆次同时采用一个轻量级数据库sqllite用于存放和FOSF存储结构相关的表信息的元数据信息，也是为何和Hive支持的数据库保持一定的松耦合性。


   3.前面提到，存储结构中使用了记录一个基本粒度的MaxMin用于在查询时候过滤掉一些无关的数据，这种索引技术是很粗糙的，在后来的性能测试中发现效果不太理想（当然，排序情况下会好很多，但不符合实际情况），简单一点就是说不少情况下where id>vlaue 这个value值不能决定一个小的page是全部满足此条件还是全部不满足，更多情况下是这个基本粒度page中的value一部分满足一部分不满足，这样的话其实就是IndexFilter没有起到过滤作用，需要逐行进行暴力比较。索引Index优化的核心就是减少这种暴力比较的情况的出现 。一个方法就是减小简建立Index的基本粒度，这样的话虽然能过滤掉更多无关的数据，但是建立更多的Index会增大存储空间和查询时候的计算代价。所以索引技术这一块，在混合存储的基础上确实没想到更优化的方法，行存储的细粒度的索引机制好像不太适合这种场景。
   
   4.在实现压缩算法的过程中，借鉴了googleProtoBuffer里面的VLQ算法，但是大家都知道Protobuffer更大的价值还在于其对结构化数据进行序列化存储的高效和简单，除了其紧凑的编码算法外，希望可以把protobuffer的序列化方式用在现有的FOSF存储结构上面。

   5.当前为每一个列选择一个编码算法是在创建表的时候定义的，问题是如果用户不知道表的数据列的分布情况，那么为其选择编码方式就是个问题。当前解决方案是为其使用一般情况下性能较好的增量位编码。后期规划是希望在数据进行加载的时候统计数据的分布情况动态的选择最适编码压缩方式。
   
HDFS_WriteBug等诸多bug
----
   https://issues.apache.org/jira/browse/HDFS-5996
   在此存储结构的实现过程中，遇到了较多的bug。影响最深的就是HDFS-Write的bug，不管充hadoop counters还是自己打印的多种记录信息，都没发现问题，全程无任何异常，最后是通过吧map结果先写到新建的临时文件，然后再move到/user、hive/warehouse/ 最终存储目录下面，方才搞定。其他问题，比如说序列化问题，变长字符串编码压缩问题行缺失问题，Index 做记录的Max和Min和实际情况不符合等
   
关于我
----
   最后，一个人做这个东西表示压力有点大，又快找工作了，还有很多其他事情要所，最近又开始了一个新的任务--hadoop生态系统自动化部署以及性能监控。。。精力实在是有限。。。。
