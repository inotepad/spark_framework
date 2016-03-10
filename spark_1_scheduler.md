# Spark中执行作业的流程
### 总体介绍
* driver端分析Job，将Job划分为多个Stage(执行先后顺序)，然后将Stage再划分成多个Task(处理不同的数据)，将Task分配给executor执行，等待执行结果
* executor收到Task后，读取数据，处理数据，返回结果给driver(有的可能写入文件)
* driver收到Task结果后，执行下一个Stage或者是结束该作业。

### （1）driver端主线程
```
sc.runJob -> dagScheduler.runJob -> waiter （JobWaiter这是一个listener）
runJob  -> SubmitJob

SubmitJob 向 eventProcessLoop 发送  JobSubmitted 消息

```
### （2）driver端 生成Task，并且提交给executor

- eventProcessLoop异步处理，这是一个线程，使用阻塞队列
- 两个进程通过该队列通信

```
dagScheduler.handleJobSubmitted
  - submitMissingTasks
    - taskScheduler.submitTasks()
      - backend.reviveOffers()
        - makeOffers
          - scheduler.resourceOffers
            - resourceOfferSingleTaskSet
              - taskSetManager.resourceOffer
        - launchTasks
        这个launchTasks就是发送把Task发送给executor，并不执行任务
```
### （3）executor中运行Task

* executor端才是执行Task的地方
* 很多人应该都会有这么一个疑问，自己写入的函数，Spark框架在哪一部分调用的?

```
CoarseGrainedExecutorBackend
LaunchTask
  - executor.launchTask
    - TaskRunner.run
      - Task.run
  - driver ! StatusUpdate
```

* 通过上面的调用可以看到，执行作业时使用的是Task中的run方法
* Spark中的Task分成两类，一类是ResultTask，一类是ShuffleMapTask。
* ResultTask

```
  override def runTask(context: TaskContext): U = {
    ...
    func(context, rdd.iterator(partition, context))
  }
  其中的fun为 stage.resultOfJob.get.func
  该函数就是自定义函数的封装fun之后的结果
    val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
  这个就是在前面的runJob和handleJobSubmitted中参数传递func函数

```

* ShuffleMapTask

```
  override def runTask(context: TaskContext): MapStatus = {
      ...
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])
      ...
  }
```

### (4) driver获得返回结果

```
backend StatusUpdate
  - taskScheduler.statusUpdate
    - taskResultGetter.enqueueSuccessfulTask
      - taskScheduler(TaskSchedulerImpl).handleSuccessfulTask
        - taskSetManager.handleSuccessfulTask
          - dagScheduler.taskEnded eventProcessLoop CompletionEvent
            - dagScheduler.handleTaskCompletion
              - ResultTask，ShuffleMapTask 错误处理，提交剩下的作业
结果成功之后
JobWaiter
  - taskSucceeded

```
