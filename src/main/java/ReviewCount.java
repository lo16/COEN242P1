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
import java.util.HashMap;
import java.util.Map;


/**
 * List each movie's rating count, sort by rating count
 */
public class ReviewCount {
    public static class MoviesMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {
        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            // The start line number, if key == 0, then the first line is csv header
            int start = 0;
            if (key.get() == 0) start = 1;

            String text = value.toString();
            String[] lines = text.split("\n");

            for (int i = start; i < lines.length; i++) {
                String line = lines[i];
                String[] parts = line.split(",");
                String title = parts[1];
                for (int j = 2; j < parts.length - 1; j++) {
                    title = title + "," + parts[i];
                }
                title = title.replaceAll("^\"|\"$", "");
                IntWritable outputKey = new IntWritable(Integer.parseInt(parts[0]));
                Text outputValue = new Text("Title#" + title);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReviewsMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {
        private Map<Integer, Integer> reviewsMap= new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            // The start line number, if key == 0, then the first line is csv header
            int start = 0;
            if (key.get() == 0) start = 1;

            String text = value.toString();
            String[] lines = text.split("\n");

            for (int i = start; i < lines.length; i++) {
                String line = lines[i];
                String[] parts = line.split(",");
                Integer movieId = Integer.parseInt(parts[1]);

                if (!reviewsMap.containsKey(movieId)) {
                    reviewsMap.put(movieId, 1);
                } else {
                    reviewsMap.put(movieId, reviewsMap.get(movieId) + 1);
                }
            }
        }

        @Override
        public void cleanup(Context con) throws IOException, InterruptedException {
            for (Integer movieId : reviewsMap.keySet()) {
                con.write(new IntWritable(movieId), new Text(reviewsMap.get(movieId) + ""));
            }
        }
    }

    public static class ReducerForReviewCount extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable movieId, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            // Count of ratings (how many people rates)
            int cnt = 0;
            // Movie title
            String title = "";

            // System.out.println(values.toString());
            for (Text value : values) {
                String tmp = value.toString();
                if (tmp.indexOf("Title#") == 0) {
                    title = tmp.substring(6);
                } else {
                    cnt += Integer.parseInt(tmp);
                }
            }
            con.write(new Text(Integer.toString(cnt)), new Text(title));
        }
    }

    public static class SortMapper extends Mapper<Text, Text, IntWritable,
            Text> {
        public void map(Text key, Text value, Context con) throws IOException,
                InterruptedException {
            con.write(new IntWritable(Integer(value.toString())), key);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable key, Text value, Context con) throws 
                IOException, InterruptedException {
            con.write(new Text(key), value);            
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // setup mapreduce job
        Job job = new Job(config, "Part 1: review count");
        job.setJarByClass(ReviewCount.class);
        // setup reducer
        job.setReducerClass(ReducerForReviewCount.class);

        // set input/output path & mapper
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path output = new Path(files[1]);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job, output);

        //sort previous job results
        Job job2 = new Job(config, "sorting output");
        job2.setJarByClass(ReviewCount.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, output);
        FileOutputFormat.setOutputPath(job2, output);

        // task completion
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
