import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
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
 * List all movies with avg rating > 4.0, and review count > 10
 */
public class ListRatings {
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
                for (int j = 2; j < parts.length - 1; j ++) {
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

        // HashMap: key - movie ID, value - amount of ratings
        private Map<Integer, Integer> cntMap= new HashMap<>();
        // HashMap: key - movie ID, value - the sum of ratings
        private Map<Integer, Double> sumMap= new HashMap<>();

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

                // Extract movie ID and rating values
                Integer movieId = Integer.parseInt(parts[1]);
                Double rating = Double.parseDouble(parts[2]);

                // Increment ratings count for this movie
                if (!cntMap.containsKey(movieId)) {
                    cntMap.put(movieId, 1);
                } else {
                    cntMap.put(movieId, cntMap.get(movieId) + 1);
                }

                // Add rating to sumMap
                if (!sumMap.containsKey(movieId)) {
                    sumMap.put(movieId, rating);
                } else {
                    sumMap.put(movieId, sumMap.get(movieId) + rating);
                }
            }
        }

        @Override
        public void cleanup(Context con) throws IOException, InterruptedException {

            // Emit the sum and count of ratings for each movie seen
            for (Integer movieId : cntMap.keySet()) {
                Double sum = sumMap.get(movieId);
                Integer cnt = cntMap.get(movieId);
                Text outputValue = new Text(String.format("%.16f\t%d", sum, cnt));
                con.write(new IntWritable(movieId), outputValue);
            }
        }
    }

    // Output of this reduce step is not sorted, which is done in the next job
    public static class Stage1Reducer extends Reducer<IntWritable, Text, Text, Text> {

        @Override
        public void reduce(IntWritable movieId, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {

            // Sum of rating points
            double sum = 0;
            // Count of ratings
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
                    // Add up sums and counts from each ReviewsMapper task
                    String[] parts = tmp.split("\t");
                    sum += Double.parseDouble(parts[0]);
                    cnt += Integer.parseInt(parts[1]);
                }
            }

            // Calculate average rating and filter out movies with low ratings
            Double avg = sum / cnt;
            if (cnt > 10 && avg > 4.0) {
                con.write(new Text(title), new Text(String.format("%.16f\t%d", avg, cnt)));
            }
        }
    }

    // Simple mapper used for sorting results
    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable,
            Text> {

        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            // Split input by row for sorting
            String[] lines = value.toString().split("\n");

            // Emit rating as key, so it can be used in the sorting step
            for (String line : lines) {
                String[] parts = line.split("\t");
                DoubleWritable rating = new DoubleWritable(Double.parseDouble(parts[1]));
                con.write(rating, new Text(parts[0] + "\t" + parts[2]));
            }
        }
    }

    // Simple reducer used for sorting results
    public static class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            for (Text value : values) {
                String[] parts = value.toString().split("\t");

                // Output rating and title
                Text outputKey = new Text(parts[0]);
                Text outputValue = new Text(String.format("%.16f\t%s", key.get(), parts[1]));
                con.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // Set up MapReduce job
        Job job1 = new Job(config, "Part 2: list ratings - Stage 1: Filter movies");
        job1.setJarByClass(ListRatings.class);
        // Set up reducer
        job1.setReducerClass(Stage1Reducer.class);

        // Set up input/output path & mappers
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path stage1output = new Path(files[1] + "/stage1");
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job1, stage1output);

        // Task completion
        int code = job1.waitForCompletion(true) ? 0 : 1;

        // Sort previous job results
        if (code == 0) {
            Job job2 = new Job(config, "Part 2: review count - Stage 2: Sort by rating");
            job2.setJarByClass(ListRatings.class);

            job2.setMapperClass(SortMapper.class);
            job2.setReducerClass(SortReducer.class);
            job2.setMapOutputKeyClass(DoubleWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // Set input/output path & mapper
            Path stage2output = new Path(files[1] + "/stage2");
            FileInputFormat.addInputPath(job2, stage1output);
            FileOutputFormat.setOutputPath(job2, stage2output);

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
