import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * List each movie's rating count, sorted by rating count
 */
public class ReviewCount {
    public static class MoviesMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {

            // If key == 0, skip the first line, which is the csv header
            int start = 0;
            if (key.get() == 0) start = 1;

            // Split the text into lines
            String text = value.toString();
            String[] lines = text.split("\n");

            // Separate the columns
            for (int i = start; i < lines.length; i++) {
                String line = lines[i];
                String[] parts = line.split(",");

                // Handle commas in movie titles
                String title = parts[1];
                for (int j = 2; j < parts.length - 1; j++) {
                    title = title + "," + parts[j];
                }
                // Remove quotation marks
                title = title.replaceAll("^\"|\"$", "");

                // Emit movie ID as key, movie title as value
                IntWritable outputKey = new IntWritable(Integer.parseInt(parts[0]));
                Text outputValue = new Text("Title#" + title);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReviewsMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {

        // HashMap used to count reviews for each movie
        private Map<Integer, Integer> reviewsMap= new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            
            // If key == 0, skip the first line, which is the csv header
            int start = 0;
            if (key.get() == 0) start = 1;

            // Split the text into lines
            String text = value.toString();
            String[] lines = text.split("\n");

            // Extract movie ID and increment count
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

        // Send movie ID and count
        @Override
        public void cleanup(Context con) throws IOException, InterruptedException {
            for (Integer movieId : reviewsMap.keySet()) {
                con.write(new IntWritable(movieId), new Text(reviewsMap.get(movieId) + ""));
            }
        }
    }

    public static class Stage1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable movieId, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            // Count of ratings (how many people rates)
            int cnt = 0;
            // Movie title
            String title = "";

            // Extract data from either mapper
            for (Text value : values) {
                String tmp = value.toString();
                if (tmp.indexOf("Title#") == 0) {
                    // Extract title from MoviesMapper
                    title = tmp.substring(6);
                } else {
                    // Add up counts from each ReviewsMapper task
                    cnt += Integer.parseInt(tmp);
                }
            }
            if (cnt > 0) {
                // Send count and title
                con.write(new IntWritable(cnt), new Text(title));
            }
        }
    }

    // Simple mapper used for sorting results
    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            // Split input by row for sorting
            String[] lines = value.toString().split("\n");

            // Emit count as key, so it can be used in the sorting step
            for (String line : lines) {
                String[] parts = line.split("\t");
                con.write(new IntWritable(Integer.parseInt(parts[0])), new Text(parts[1]));
            }
        }
    }

    // Simple reducer used for sorting results
    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable cnt, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            for (Text value : values) {
                // Output count and title
                con.write(cnt, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.nanoTime();

        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // Set up MapReduce job
        Job job1 = new Job(config, "Part 1: review count - Stage 1: Get counts of each movie");
        job1.setJarByClass(ReviewCount.class);
        // Set up reducer
        job1.setReducerClass(Stage1Reducer.class);

        // Set up input/output path & mappers
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path stage1output = new Path(files[1] + "/stage1");
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job1, stage1output);

        // Task completion
        int code = job1.waitForCompletion(true) ? 0 : 1;

        // Sort previous job results
        if (code == 0) {
            Job job2 = new Job(config, "Part 1: review count - Stage 2: Sort by counts");
            job2.setJarByClass(ReviewCount.class);

            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            // Set input/output path & mapper
            Path stage2output = new Path(files[1] + "/stage2");
            FileInputFormat.addInputPath(job2, stage1output);
            FileOutputFormat.setOutputPath(job2, stage2output);

            int code2 = job2.waitForCompletion(true) ? 0 : 1;
            long endTime   = System.nanoTime();
            long totalTime = endTime - startTime;
            System.out.println("Total real time spent: " + totalTime / 1000000000.0 + "s");
            System.exit(code2);
        } else {
            System.exit(1);
        }
    }
}
