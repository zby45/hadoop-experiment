import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InvertedIndex {

    // Mapper：分词、过滤停词、输出 <单词, 文件名:1>
    public static class IndexMapper extends Mapper<Object, Text, Text, Text> {
        private final Set<String> stopWords = new HashSet<>();
        private final Text wordKey = new Text();
        private final Text docValue = new Text();

        // Hadoop 3.x 兼容的停词表加载方式
        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path stopWordPath = new Path(cacheFiles[0]);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(
                    new InputStreamReader(fs.open(stopWordPath))
                );
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前处理的文件名
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // 转小写，按非字母字符拆分单词
            String[] words = value.toString().toLowerCase().split("[^a-zA-Z]+");

            for (String word : words) {
                // 过滤空串和停词
                if (word.length() > 0 && !stopWords.contains(word)) {
                    wordKey.set(word);
                    docValue.set(fileName + ":1");
                    context.write(wordKey, docValue);
                }
            }
        }
    }

    // Reducer：聚合词频，按要求格式输出
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> docCountMap = new HashMap<>();
            int total = 0;

            // 统计每个文档中该单词的出现次数
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                String docName = parts[0];
                int count = Integer.parseInt(parts[1]);
                docCountMap.put(docName, docCountMap.getOrDefault(docName, 0) + count);
                total += count;
            }

            // 按实验要求格式拼接：<doc1,n>;<doc2,m>;<total,sum>.
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Map.Entry<String, Integer> entry : docCountMap.entrySet()) {
                if (!first) {
                    sb.append(";");
                }
                sb.append("<").append(entry.getKey()).append(",").append(entry.getValue()).append(">");
                first = false;
            }
            sb.append(";<total,").append(total).append(">.");

            result.set(sb.toString());
            context.write(key, result);
        }
    }

    // 主程序：配置任务、提交运行
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted_index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setReducerClass(IndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 加载停词表到分布式缓存
        job.addCacheFile(new Path(args[2]).toUri());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
