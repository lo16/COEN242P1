import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * List all movies with avg rating > 4.0
 */
public class ListRatings {
    public static class MoviesMapper extends Mapper<LongWritable, Text, Text,
            Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            String text = value.toString();
            String[] lines = text.split("\n");
            for (String line : lines) {
                String[] parts = line.split(",");
                Text outputKey = new Text(parts[0]);
                Text outputValue = new Text("Title#" + parts[1]);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReviewsMapper extends Mapper<LongWritable, Text, Text,
            Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            String text = value.toString();
            String[] lines = text.split("\n");
            for (String line : lines) {
                String[] parts = line.split(",");
                Text outputKey = new Text(parts[1]);
                Text outputValue = new Text(parts[2]);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReducerForListRatings extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text movieId, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            // Sum of rating points
            double sum = 0;
            // Count of ratings (how many people rates)
            int cnt = 0;
            // Movie title
            String title = "";

            // System.out.println(values.toString());
            for (Text value : values) {
                String[] parts = value.toString().split("#");
                if (parts[0].equals("Title")) {
                    System.out.println(value.toString());
                    title = parts[1];
                } else {
                    if (!parts[0].equals("movieId") && !parts[0].equals("rating")) {
                        sum += Double.parseDouble(parts[0]);
                        cnt += 1;
                    }
                }
            }
            if (cnt >= 10 && sum / cnt >= 4.0) {
                con.write(new Text(title), new Text(String.format("%f\t%d", sum / cnt, cnt)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // setup mapreduce job
        Job job = new Job(config, "Part 2: list ratings");
        job.setJarByClass(ListRatings.class);
        // setup reducer
        job.setReducerClass(ReducerForListRatings.class);

        // set input/output path & mapper
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path output = new Path(files[1]);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        // task completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
