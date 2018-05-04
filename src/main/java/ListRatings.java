import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * List all movies with avg rating > 4.0
 */
public class ListRatings {
    public static class MapForListRatings extends Mapper<LongWritable, Text, Text,
            IntWritable> {
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");
            IntWritable outputValue = new IntWritable(1);
            for (String word : words) {
                Text outputKey = new Text(word.toUpperCase().trim());
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReduceForListRatings extends Reducer<Text, IntWritable, Text,
            IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) throws
                IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
// Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();
// setup mapreduce job
        Job job = new Job(config, "listratings");
        job.setJarByClass(ListRatings.class);
// setup mapper
        job.setMapperClass(MapForListRatings.class);
// setup reducer
        job.setReducerClass(ReduceForListRatings.class);
// set input/output path
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
// task completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
