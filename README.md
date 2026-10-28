# Hadoop MapReduce 倒排索引实验

## 实验内容
基于 Hadoop 3.x 伪分布式环境，实现英文文档倒排索引的构建，完成分词、停词过滤、词频统计全流程，输出格式符合课程实验要求。

## 文件说明
| 文件名 | 作用 |
|--------|------|
| `InvertedIndex.java` | MapReduce 核心代码，包含 Mapper 分词过滤、Reducer 聚合统计完整实现 |
| `stopwords.txt` | 英文停词表，用于过滤无意义高频功能词 |

## 运行方式
```bash
# 先编译打包
javac -cp $(hadoop classpath) InvertedIndex.java
jar -cvf inverted-index.jar *.class

# 提交MapReduce任务
hadoop jar inverted-index.jar InvertedIndex <HDFS输入路径> <HDFS输出路径> <停词表HDFS路径>
```

## 输出格式
单词按字母序排列，每个单词对应出现的文档及词频，最后统计总出现次数：
```
word  <doc1.txt,2>;<doc3.txt,1>;<total,3>.
```
