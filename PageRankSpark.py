import sys
from decimal import Decimal, ROUND_HALF_UP
from pyspark.sql import SparkSession

# PageRank 参数：阻尼因子0.85，4个节点，迭代10轮
DAMPING = 0.85
NODE_COUNT = 4
ITERATIONS = 10
INIT_PR = 1.0 / NODE_COUNT

def format_pr(pr):
    # 保留10位小数
    return Decimal(str(pr)).quantize(Decimal("0.0000000000"), rounding=ROUND_HALF_UP)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PageRank-PySpark") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # 读取图结构：(节点, 邻居列表)
    graph = sc.textFile(input_path) \
        .map(lambda line: line.strip().split("\t")) \
        .map(lambda x: (x[0].trim(), [n.strip() for n in x[1].split(",")])) \
        .cache()

    # 初始化每个节点PR值
    rank = graph.mapValues(lambda _: INIT_PR).cache()

    # 迭代10轮更新PR
    for i in range(1, ITERATIONS + 1):
        contributions = graph.join(rank) \
            .flatMap(lambda x: [(nb, x[1][1] / len(x[1][0])) for nb in x[1][0]])
        rank = contributions.reduceByKey(lambda a, b: a + b) \
            .mapValues(lambda s: (1 - DAMPING) / NODE_COUNT + DAMPING * s) \
            .cache()

    # 格式化输出：(网页, PR值)，PR保留10位小数
    result = rank.map(lambda x: f"({x[0]}, {format_pr(x[1])})")
    result.saveAsTextFile(output_path)

    # 控制台打印最终结果
    for item in result.collect():
        print(item)

    spark.stop()
