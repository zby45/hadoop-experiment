# Hadoop Experiment Code

山东大学 大数据管理与分析 实验一代码

## 环境
- Ubuntu 22.04 64位
- Hadoop 3.3.6 (伪分布式)
- OpenJDK 1.8

## 代码说明
| 文件 | 功能 |
|---|---|
| MergeDedup.java | 文件合并与去重 |
| SortMR.java | 整数升序排序（输出位次） |
| GrandRelation.java | 祖孙关系挖掘 |

## 编译运行
```bash
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main *.java
jar cf experiment.jar *.class
hadoop jar experiment.jar MergeDedup /input /output
```
