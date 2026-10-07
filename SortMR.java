import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortMR {
    public static class SortMapper extends Mapper<Object, org.apache.hadoop.io.Text, IntWritable, IntWritable> {
        private IntWritable num = new IntWritable();
        public void map(Object key, org.apache.hadoop.io.Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (!s.isEmpty()) {
                num.set(Integer.parseInt(s));
                context.write(num, new IntWritable(1));
            }
        }
    }
    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable rank = new IntWritable(1);
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(rank, key);
                rank.set(rank.get() + 1);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortMR");
        job.setJarByClass(SortMR.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
