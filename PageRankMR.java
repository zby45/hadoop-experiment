import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * PageRank MapReduce 实现
 * 输入格式：节点\t邻居1,邻居2,...
 * 迭代10轮，阻尼因子 d=0.85，初始PR=1/4
 * 最终输出：(网页, PR值)，PR保留10位小数
 */
public class PageRankMR {

    public static final double DAMPING = 0.85;
    public static final int NODE_COUNT = 4;
    public static final double INIT_PR = 1.0 / NODE_COUNT;
    public static final int ITERATIONS = 10;

    /**
     * 第一轮：解析原始图，初始化PR值
     * 输入：A\tB,C
     * 输出：A\t0.25\tB,C
     */
    public static class InitMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            String node = parts[0].trim();
            String neighbors = parts.length > 1 ? parts[1].trim() : "";
            outKey.set(node);
            outValue.set(INIT_PR + "\t" + neighbors);
            context.write(outKey, outValue);
        }
    }

    /**
     * 迭代阶段Mapper：
     * 1. 保留图结构（邻接列表）
     * 2. 将当前PR按出链数平分，向每个邻居发送PR贡献
     */
    public static class RankMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            String node = parts[0].trim();
            double pr = Double.parseDouble(parts[1].trim());
            String neighbors = parts.length > 2 ? parts[2].trim() : "";

            // 输出图结构，供Reducer保留邻接关系
            outKey.set(node);
            outValue.set("neighbors:" + neighbors);
            context.write(outKey, outValue);

            // 按出链数平分PR，向邻居发送贡献
            if (!neighbors.isEmpty()) {
                String[] neighborArr = neighbors.split(",");
                double share = pr / neighborArr.length;
                for (String nb : neighborArr) {
                    nb = nb.trim();
                    if (nb.isEmpty()) continue;
                    outKey.set(nb);
                    outValue.set("rank:" + share);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    /**
     * 迭代阶段Reducer：聚合所有入链贡献，计算新PR
     */
    public static class RankReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumRank = 0.0;
            String neighbors = "";
            for (Text v : values) {
                String val = v.toString();
                if (val.startsWith("neighbors:")) {
                    neighbors = val.substring("neighbors:".length());
                } else if (val.startsWith("rank:")) {
                    sumRank += Double.parseDouble(val.substring("rank:".length()));
                }
            }
            // 标准PageRank公式：PR = (1-d)/N + d * sum(PR(j)/out(j))
            double newPr = (1 - DAMPING) / NODE_COUNT + DAMPING * sumRank;
            outKey.set(key);
            outValue.set(newPr + "\t" + neighbors);
            context.write(outKey, outValue);
        }
    }

    /**
     * 最终格式化输出：(网页, PR值)，保留10位小数
     */
    public static class FormatMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private DecimalFormat df = new DecimalFormat("0.0000000000");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split("\t");
            String node = parts[0].trim();
            double pr = Double.parseDouble(parts[1].trim());
            outKey.set("(" + node + ",");
            outValue.set(df.format(pr) + ")");
            context.write(outKey, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String basePath = args[1];

        // 第一轮：初始化图与PR
        Job initJob = Job.getInstance(conf, "PageRank-Init");
        initJob.setJarByClass(PageRankMR.class);
        initJob.setMapperClass(InitMapper.class);
        initJob.setOutputKeyClass(Text.class);
        initJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(initJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(initJob, new Path(basePath + "/iter0"));
        initJob.waitForCompletion(true);

        // 迭代10轮
        for (int i = 1; i <= ITERATIONS; i++) {
            Job rankJob = Job.getInstance(conf, "PageRank-Iter-" + i);
            rankJob.setJarByClass(PageRankMR.class);
            rankJob.setMapperClass(RankMapper.class);
            rankJob.setReducerClass(RankReducer.class);
            rankJob.setOutputKeyClass(Text.class);
            rankJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(rankJob, new Path(basePath + "/iter" + (i - 1)));
            FileOutputFormat.setOutputPath(rankJob, new Path(basePath + "/iter" + i));
            rankJob.waitForCompletion(true);
        }

        // 格式化输出最终结果
        Job formatJob = Job.getInstance(conf, "PageRank-Format");
        formatJob.setJarByClass(PageRankMR.class);
        formatJob.setMapperClass(FormatMapper.class);
        formatJob.setOutputKeyClass(Text.class);
        formatJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(formatJob, new Path(basePath + "/iter" + ITERATIONS));
        FileOutputFormat.setOutputPath(formatJob, new Path(basePath + "/final"));
        formatJob.waitForCompletion(true);

        System.out.println("PageRank 迭代" + ITERATIONS + "轮计算完成，结果输出到 " + basePath + "/final");
    }
}
