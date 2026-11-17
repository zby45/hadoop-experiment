import org.apache.spark.{SparkConf, SparkContext}
import java.text.DecimalFormat

/**
 * PageRank Spark 版本实现
 * 输入：HDFS上的图文件，每行 节点\t邻居1,邻居2,...
 * 迭代10轮，阻尼因子0.85，初始PR=1/4
 * 输出：(网页, PR值)，保留10位小数
 */
object PageRankSpark {
  def main(args: Array[String]): Unit = {
    val DAMPING = 0.85
    val NODE_COUNT = 4
    val ITERATIONS = 10
    val INIT_PR = 1.0 / NODE_COUNT

    val conf = new SparkConf().setAppName("PageRank-Spark")
    val sc = new SparkContext(conf)

    // 读取图结构：(节点, 邻居数组)
    val graph = sc.textFile(args(0))
      .map(line => {
        val parts = line.trim.split("\t")
        val node = parts(0).trim
        val neighbors = if (parts.length > 1) parts(1).trim.split(",").map(_.trim) else Array[String]()
        (node, neighbors)
      }).cache()

    // 初始化PR值
    var rank = graph.mapValues(_ => INIT_PR).cache()

    // 迭代10轮
    for (i <- 1 to ITERATIONS) {
      // 图结构join当前PR，分发贡献值
      val contributions = graph.join(rank)
        .flatMap { case (_, (neighbors, pr)) =>
          val share = pr / neighbors.length
          neighbors.map(nb => (nb, share))
        }
      // 聚合贡献，计算新PR
      rank = contributions.reduceByKey(_ + _)
        .mapValues(sum => (1 - DAMPING) / NODE_COUNT + DAMPING * sum)
        .cache()
    }

    // 格式化输出，保留10位小数
    val df = new DecimalFormat("0.0000000000")
    val result = rank.map { case (node, pr) =>
      "(" + node + ", " + df.format(pr) + ")"
    }

    // 结果保存到指定输出路径
    result.saveAsTextFile(args(1))

    // 控制台打印最终结果
    result.collect().foreach(println)
    sc.stop()
  }
}
