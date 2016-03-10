# Spark中pipeline

### 问题的引入 
在前面我们已经看到了Spark框架是如何调用自定义的函数的。现在仍然存在的问题就是 如果是多次Transformation，那么是为什么能够遍历一次数据，pipeline这些Transformation，一次执行完所有的函数呢？

*  https://www.zhihu.com/question/23079001/answer/23569986

如果我们假设map的意义是一次遍历，并且立即执行。写的spark的代码应该是map(f1).map(f2) ，最终执行的时候是map(f2(f1)) 


下面我们以first为例，讲解如何pipeline多个Transformation，也就是前面答案中的

> spark在运行时，动态构造了一个复合Iterator

但是这个是如何构造出来的呢？

### 复合Iterator的构造

```
val rdd2 = rdd1.map(f1)
val rdd3 = rdd2.map(f2)
val element=rdd3.first
```
##### driver端的Task构造
首先
```
  def first(): T = take(1) match {
    case Array(t) => t
    case _ => throw new UnsupportedOperationException("empty collection")
  }

```
此时可以简单的认为
```
val element = rdd3.take(1)
```
此时take开始提交作业

```
  def take(num: Int): Array[T] = {
      ...
      val res = sc.runJob(this, (it: Iterator[T]) => it.take    (left).toArray, p, allowLocal = true)
      ...
    }
```
其中
```
  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit) = {
        dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
        resultHandler, localProperties.get)
      }
那么在runJob中
    参数rdd = this
    参数func = 
    (context: TaskContext, iter: Iterator[T]) => iter.take(left).toArray
    参数resultHandler (index, res) => results(index) = res(it: Iterator[T]) => it.take(left).toArray
```



```
  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties = null)
  {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      runLocally(job)
  }
  提交作业后，被handleJobSubmitted处理，由于first作业比较简单，可以直接在driver端处理，进入 runLocally(job)函数
```

此时的作业变成了

```
val element = rdd3.take(1).toArray
```

##### 作业执行

```
  protected def runLocallyWithinThread(job: ActiveJob) {
      val result = job.func(taskContext, rdd.iterator(split, taskContext))
  }
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
      computeOrReadCheckpoint(split, context)
  }
    private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointed) firstParent[T].iterator(split, context) else compute(split, context)
  }
```

* 看到可以可以知道会调用compute函数。
* 这里的rdd2、rdd3均为MapPartitionsRDD，它的compute函数如下

```
  private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override def compute(split: Partition, context: TaskContext) =
    f(context, split.index, firstParent[T].iterator(split, context))
}
  def map[U: ClassTag](f: T => U): RDD[U] = {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```

以上变化就是
```
val element = rdd3.compute.take(1).toArray
val element = rdd2.iterator.map(f2).take(1).toArray
val element = rdd2.compute.map(f2).take(1).toArray
val element = rdd1.iterator().map(f1).map(f2).take(1).toArray
val element = rdd1.compute().map(f1).map(f2).take(1).toArray
```
假设rdd1是一个hadoopRDD，那么根据它的compute函数，可以看出也就是返回一个Iterator

最终的结果

```
val element = iterator.map(f1).map(f2).take(1).toArray
```

### 惰性Iterator

查看scala的代码

https://github.com/scala/scala/blob/2.12.x/src/library/scala/collection/Iterator.scala

```
  def map[B](f: A => B): Iterator[B] = new AbstractIterator[B] {
    def hasNext = self.hasNext
    def next() = f(self.next())
  }


  def filter(p: A => Boolean): Iterator[A] = new AbstractIterator[A] {
        private var hd: A = _
    private var hdDefined: Boolean = false

    def hasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hd = self.next()
      } while (!p(hd))
      hdDefined = true
      true
    }
    def next() = if (hasNext) { hdDefined = false; hd } else empty.next()
  }
```
Iterator本身是lazy的，就是说它并不运算，仅仅是返回一个Iterator就结束。
那么我们前面的代码就变成了
```
iterator.map(x=>f2(f1(x))).take(1).toArray
```

使用scala代码作为例子就是

```
$ scala
Welcome to Scala version 2.10.4 (OpenJDK 64-Bit Server VM, Java 1.7.0_65).
Type in expressions to have them evaluated.
Type :help for more information.

scala> Iterator(1,2,3).map(_+1)
res0: Iterator[Int] = non-empty iterator

scala> Iterator(1,2,3).map(_+1).map(_+2)
res1: Iterator[Int] = non-empty iterator

scala> Iterator(1,2,3).map(_+1).map(_+2).take(1)
res2: Iterator[Int] = non-empty iterator

scala> Iterator(1,2,3).map(_+1).map(_+2).take(1).toArray
res3: Array[Int] = Array(4)

```

### 总结

* 输入代码

```
val rdd2 = rdd1.map(f1)
val rdd3 = rdd2.map(f2)
val element=rdd3.first
```

* spark的复合Iterator

```
val element = rdd3.compute.take(1).toArray
val element = rdd2.iterator.map(f2).take(1).toArray
val element = rdd2.compute.map(f2).take(1).toArray
val element = rdd1.iterator().map(f1).map(f2).take(1).toArray
val element = rdd1.compute().map(f1).map(f2).take(1).toArray
val element = iterator().map(f1).map(f2).take(1).toArray
```

* scala的惰性Iterator

```
val element = iterator.map(x=>f2(f1(x))).take(1).toArray
```
