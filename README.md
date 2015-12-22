自定义的flume interceptor, source

# 引用第三方jar包
在flume目录下新建plugins.d目录，此目录下每个插件单独一个目录，每个插件目录下可以有lib,libext,native。
* lib: 放置插件jar包
* libext: 放置插件引用的jar包
* native: 放置所需的native库, 比如.so文件

现在有三个插件，一是引用hdfs的jar包，二是自己实现的flume interceptor，三是hadoop lzo，plugins.d目录结构如下：

```
plugins.d/
plugins.d/custom/
plugins.d/custom/lib/flume-agent-1.0-SNAPSHOT.jar
plugins.d/hadoop/
plugins.d/hadoop/lib/commons-configuration-1.6.jar
plugins.d/hadoop/lib/hadoop-auth-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-common-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-hdfs-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-nfs-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/htrace-core-3.0.4.jar
plugins.d/hadoop-lzo/
plugins.d/hadoop-lzo/lib/hadoop-lzo-cdh4-0.4.15-gplextras.jar
plugins.d/hadoop-lzo/native/libgplcompression.a
plugins.d/hadoop-lzo/native/libgplcompression.la
plugins.d/hadoop-lzo/native/libgplcompression.so
plugins.d/hadoop-lzo/native/libgplcompression.so.0
plugins.d/hadoop-lzo/native/libgplcompression.so.0.0.0
```

# flume配置

## flume上报到HDFS
参考`conf/flume.hdfs.conf`

### Interceptor
使用`com.firstshare.flume.interceptor.HDFSInterceptor$Builder`, 主要是在header中加入时间,文件名,ip, 供`hdfs-sink`使用.

配置示例:
```
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = com.firstshare.flume.interceptor.HDFSInterceptor$Builder
a1.sources.r1.interceptors.i1.hdfsinterceptor.switch = true
```

### Source
使用`com.firstshare.flume.source.SpoolDirectoryHourlySource`, 基于`SpoolDirectorySource`修改.

增加如下功能:
1. 日志目录(logDir)与flume监控目录(spoolDir)分离
2. 每小时将上一个小时的日志从`logDir`复制到`spoolDir`中
3. 上报完成后对日志进行压缩, 可选`gz`和`zip`
4. 自动删除N天前日志
5. 日志文件名需要遵守一定格式,主要是时间, demo: "ChatServer-GroupChatBoard-v2-2015111014.log"

配置示例:

```
a1.sources.r1.type = com.firstshare.flume.source.SpoolDirectoryHourlySource
a1.sources.r1.logDir = /data/appStatLog
a1.sources.r1.spoolDir = /data/appStatLog/flume
a1.sources.r1.fileSuffix = .tmp
a1.sources.r1.ignorePattern = ^(.)*\\.tmp$
a1.sources.r1.deletePolicy = immediate
a1.sources.r1.basenameHeader = true
a1.sources.r1.basenameHeaderKey = file
# 整小时过多长时间后进行日志处理
a1.sources.r1.rollMinutes = 1
a1.sources.r1.filePrefix = fs-app-center-web-
a1.sources.r1.fileCompressionMode = gz
# 日志存放最长时间, 单位"天"
a1.sources.r1.fileMaxHistory = 7
```

### hdfs sink

将所需jar放在plugins.d/hadoop/lib/目录下

```
plugins.d/hadoop/lib/commons-configuration-1.6.jar
plugins.d/hadoop/lib/hadoop-auth-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-common-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-hdfs-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/hadoop-nfs-2.6.0-cdh5.4.8.jar
plugins.d/hadoop/lib/htrace-core-3.0.4.jar
```

### 上报hdfs所需其他配置

#### 上报时使用lzo压缩
1. 确保cloudera中已经激活hadoop_lzo的parcel，并且hdfs/yarn也支持lzo。安装步骤参见Cloudera配置hadoop_lzo
2. 手工编译lzo和hadoop-lzo的，直接将jar包放在plugins.d下。使用Cloudera安装hadoop-lzo parcel的，要将hadoop_lzo的jar包和native下库连接都放在plugins.d下。
3. 从hadoop集群上拉取`core-site.xml`放在`flume/conf`下，确保`io.compression.codecs`中存在`com.hadoop.compression.lzo.LzoCodec`和`com.hadoop.compression.lzo.LzopCodec`
4. flume的hdfs sink中配置

```
a1.sinks.hdfs-sink.hdfs.fileType = CompressedStream
a1.sinks.hdfs-sink.hdfs.codeC = com.hadoop.compression.lzo.LzopCodec
```

#### HDFS HA
主备namenode状态发生变化时能够自动切换，上报hdfs不受影响。

1. 确保hadoop集群已经开启HA机制。
2. 将集群中`hdfs-site.xml`放在`flume/conf`下。
3. `hdfs-site.xml`中有许多是vlnx107009等形式的，要替换回ip。
4. flume的hdfs sink中配置

```
## 修改前
# a1.sinks.hdfs-sink.hdfs.path = hdfs://{active namenode ip}/facishare-data/app/center/web/%{year}/%{month}/%{day}/
## 修改后
a1.sinks.hdfs-sink.hdfs.path = /facishare-data/app/center/web/%{year}/%{month}/%{day}/
```

更多flume上报hdfs的内容可以参考[http://wiki.firstshare.cn/pages/viewpage.action?pageId=18645082](http://wiki.firstshare.cn/pages/viewpage.action?pageId=18645082)

## flume上报到kafka

### Interceptor

使用`com.firstshare.flume.source.DirTailPollableSource2`, 能够动态tail目录下最后修改的文件

功能:
1. 根据`filePrefix`监控目录下最新修改的文件进行tail, 能够自动切换
2. 在每行日志最后添加`appName`供oss使用, 中间使用'\u0001'隔断.
3. 需要监控多个目录或不同前缀的文件,需要配置多个source, 各source之间相互不影响

配置示例:

```
a1.sources.r1.type = com.firstshare.flume.source.DirTailPollableSource2
a1.sources.r1.path = /opt/ngx_log
a1.sources.r1.filePrefix = www.fxk-
a1.sources.r1.appName = nginx-fxk
```
