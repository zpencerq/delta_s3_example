# delta_s3_example
Simple example of Delta S3 write to mocked S3 and errors

This boots up a mocked S3 server using `moto` and creates a SparkSession pointing at that `moto_server` to demonstrate writing using delta format.

Just run `./run.sh`.


## The stacktrace

```
Traceback (most recent call last):
  File "test.py", line 39, in <module>
    run(session)
  File "test.py", line 23, in run
    .save("s3a://test-bucket/foo/delta")
  File "/Volumes/Workspace/delta_and_s3_example/venv/lib/python3.6/site-packages/pyspark/sql/readwriter.py", line 734, in save
    self._jwrite.save(path)
  File "/Volumes/Workspace/delta_and_s3_example/venv/lib/python3.6/site-packages/py4j/java_gateway.py", line 1257, in __call__
    answer, self.gateway_client, self.target_id, self.name)
  File "/Volumes/Workspace/delta_and_s3_example/venv/lib/python3.6/site-packages/pyspark/sql/utils.py", line 63, in deco
    return f(*a, **kw)
  File "/Volumes/Workspace/delta_and_s3_example/venv/lib/python3.6/site-packages/py4j/protocol.py", line 328, in get_return_value
    format(target_id, ".", name), value)
py4j.protocol.Py4JJavaError: An error occurred while calling o92.save.
: java.util.concurrent.ExecutionException: org.apache.hadoop.fs.UnsupportedFileSystemException: fs.AbstractFileSystem.s3a.impl=null: No AbstractFileSystem configured for scheme: s3a
        at com.google.common.util.concurrent.AbstractFuture$Sync.getValue(AbstractFuture.java:306)
        at com.google.common.util.concurrent.AbstractFuture$Sync.get(AbstractFuture.java:293)
        at com.google.common.util.concurrent.AbstractFuture.get(AbstractFuture.java:116)
        at com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly(Uninterruptibles.java:135)
        at com.google.common.cache.LocalCache$Segment.getAndRecordStats(LocalCache.java:2410)
        at com.google.common.cache.LocalCache$Segment.loadSync(LocalCache.java:2380)
        at com.google.common.cache.LocalCache$Segment.lockedGetOrLoad(LocalCache.java:2342)
        at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2257)
        at com.google.common.cache.LocalCache.get(LocalCache.java:4000)
        at com.google.common.cache.LocalCache$LocalManualCache.get(LocalCache.java:4789)
        at org.apache.spark.sql.delta.DeltaLog$.apply(DeltaLog.scala:721)
        at org.apache.spark.sql.delta.DeltaLog$.forTable(DeltaLog.scala:653)
        at org.apache.spark.sql.delta.sources.DeltaDataSource.createRelation(DeltaDataSource.scala:139)
        at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:45)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:86)
        at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:131)
        at org.apache.spark.sql.execution.SparkPlan$$anonfun$execute$1.apply(SparkPlan.scala:127)
        at org.apache.spark.sql.execution.SparkPlan$$anonfun$executeQuery$1.apply(SparkPlan.scala:155)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:152)
        at org.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:127)
        at org.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:80)
        at org.apache.spark.sql.execution.QueryExecution.toRdd(QueryExecution.scala:80)
        at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:676)
        at org.apache.spark.sql.DataFrameWriter$$anonfun$runCommand$1.apply(DataFrameWriter.scala:676)
        at org.apache.spark.sql.execution.SQLExecution$$anonfun$withNewExecutionId$1.apply(SQLExecution.scala:78)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:73)
        at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:676)
        at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:285)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:271)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:229)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:497)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
        at py4j.Gateway.invoke(Gateway.java:282)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.GatewayConnection.run(GatewayConnection.java:238)
        at java.lang.Thread.run(Thread.java:745)
Caused by: org.apache.hadoop.fs.UnsupportedFileSystemException: fs.AbstractFileSystem.s3a.impl=null: No AbstractFileSystem configured for scheme: s3a
        at org.apache.hadoop.fs.AbstractFileSystem.createFileSystem(AbstractFileSystem.java:160)
        at org.apache.hadoop.fs.AbstractFileSystem.get(AbstractFileSystem.java:249)
        at org.apache.hadoop.fs.FileContext$2.run(FileContext.java:334)
        at org.apache.hadoop.fs.FileContext$2.run(FileContext.java:331)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1698)
        at org.apache.hadoop.fs.FileContext.getAbstractFileSystem(FileContext.java:331)
        at org.apache.hadoop.fs.FileContext.getFileContext(FileContext.java:448)
        at org.apache.spark.sql.delta.storage.HDFSLogStoreImpl.getFileContext(HDFSLogStoreImpl.scala:52)
        at org.apache.spark.sql.delta.storage.HDFSLogStoreImpl.read(HDFSLogStoreImpl.scala:56)
        at org.apache.spark.sql.delta.Checkpoints$class.loadMetadataFromFile(Checkpoints.scala:138)
        at org.apache.spark.sql.delta.Checkpoints$class.lastCheckpoint(Checkpoints.scala:132)
        at org.apache.spark.sql.delta.DeltaLog.lastCheckpoint(DeltaLog.scala:59)
        at org.apache.spark.sql.delta.DeltaLog.<init>(DeltaLog.scala:141)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3$$anonfun$call$1$$anonfun$apply$7.apply(DeltaLog.scala:725)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3$$anonfun$call$1$$anonfun$apply$7.apply(DeltaLog.scala:725)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper$.allowInvokingTransformsInAnalyzer(AnalysisHelper.scala:194)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3$$anonfun$call$1.apply(DeltaLog.scala:724)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3$$anonfun$call$1.apply(DeltaLog.scala:724)
        at com.databricks.spark.util.DatabricksLogging$class.recordOperation(DatabricksLogging.scala:75)
        at org.apache.spark.sql.delta.DeltaLog$.recordOperation(DeltaLog.scala:626)
        at org.apache.spark.sql.delta.metering.DeltaLogging$class.recordDeltaOperation(DeltaLogging.scala:105)
        at org.apache.spark.sql.delta.DeltaLog$.recordDeltaOperation(DeltaLog.scala:626)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3.call(DeltaLog.scala:723)
        at org.apache.spark.sql.delta.DeltaLog$$anon$3.call(DeltaLog.scala:721)
        at com.google.common.cache.LocalCache$LocalManualCache$1.load(LocalCache.java:4792)
        at com.google.common.cache.LocalCache$LoadingValueReference.loadFuture(LocalCache.java:3599)
        at com.google.common.cache.LocalCache$Segment.loadSync(LocalCache.java:2379)
        ... 39 more

```
