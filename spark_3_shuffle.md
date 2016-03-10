# Spark中的shuffle

### 1.Spark shuffle总览
* Pluggable interface for shuffles

https://issues.apache.org/jira/browse/SPARK-2044

Spark的shuffle部分有相当复杂的优化过程。具体的一些分析可以参考下面的一些内容，这里不再赘述。1.5中引入和tungsten-sort，1.6中变的更加复杂。


* hash shuffle 以及改进

http://jerryshao.me/architecture/2014/01/04/spark-shuffle-detail-investigation/

https://github.com/JerryLead/SparkInternals/blob/master/markdown/4-shuffleDetails.md

* sort shuffle 

http://www.jianshu.com/p/c83bb237caa8

http://www.jianshu.com/p/2d837bf2dab6

* tungsten-sort

http://www.jianshu.com/p/d328c96aebfd



### 2.Spark shuffle的原理

mapreduce中的shuffle是一个分布式的排序过程，通过排序将相同key的value聚集在一起，中间执行一些combine之类的操作。spark的shuffle是以hash为核心的，通过hash将相同的key的value聚集在一起。这里面临的一个问题就是大数据量下如何执行？

map的输出部分，mapreduce是写入内存缓冲区，然后在排序之后执行combine操作，相关数据结构就是MapOutputBuffer。而Spark中则是使用AppendOnlyMap来存储这些数据，这个可以极大的加快Aggregator相关的操作

对于大规模数据排序而言，有良好的外部排序算法，可能处理大规模数据。而hash则难以利用硬盘等来处理大规模数据。spark在处理这个问题是使用的是ExternalAppendOnlyMap，具体做法是按照key的hash排序。对于partition而言，如果partition的数量不大，可以直接一个parition一个文件，如果文件数量过多，则可以借鉴mapreduce中的merge过程，保证相同key的value聚集在一起的前提下，不断的合并文件，就是在ExternalSorter中的实现。只不过这里是只对partition进行了排序，对于key按照hash排序。

如果是不需要Aggregator操作则无法利用到hash的优势。目前tungsten-sort应该可以比较适合这种类型的操作，例如groupByKey等。目前而且根据论文*Clash of the titans: MapReduce vs. Spark for large scale data analytics*中的描述，mapreduce的shuffle和map是可以同时进行的，这个也是值得Spark中借鉴的。
