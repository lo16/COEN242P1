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
 * List all movies with avg rating >= 4.0, and review cnt > 10
 */
public class ListRatings {
    public static class MoviesMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {
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
                for (int j = 2; j < parts.length - 1; j ++) {
                    title = title + "," + parts[j];
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
        // HashMap: key - movie Id, value - how many people rate
        private Map<Integer, Integer> cntMap= new HashMap<>();
        // HashMap: key - movie Id, value - the sum of ratings
        private Map<Integer, Double> sumMap= new HashMap<>();

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
                Double rating = Double.parseDouble(parts[2]);

                if (!cntMap.containsKey(movieId)) {
                    cntMap.put(movieId, 1);
                } else {
                    cntMap.put(movieId, cntMap.get(movieId) + 1);
                }

                if (!sumMap.containsKey(movieId)) {
                    sumMap.put(movieId, rating);
                } else {
                    sumMap.put(movieId, sumMap.get(movieId) + rating);
                }
            }
        }

        @Override
        public void cleanup(Context con) throws IOException, InterruptedException {
            for (Integer movieId : cntMap.keySet()) {
                Double sum = sumMap.get(movieId);
                Integer cnt = cntMap.get(movieId);
                Text outputValue = new Text(String.format("%.16f\t%d", sum, cnt));
                con.write(new IntWritable(movieId), outputValue);
            }
        }
    }

    public static class Stage1Reducer extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable movieId, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            // Sum of rating points
            double sum = 0;
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
                    String[] parts = tmp.split("\t");
                    sum += Double.parseDouble(parts[0]);
                    cnt += Integer.parseInt(parts[1]);
                }
            }
            Double avg = sum / cnt;
            if (cnt > 10 && avg > 4.0) {
                con.write(new Text(title), new Text(String.format("%.16f\t%d", avg, cnt)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // Setup mapreduce job
        Job job1 = new Job(config, "Part 2: list ratings - Stage 1: Filter movies");
        job1.setJarByClass(ListRatings.class);
        // setup reducer
        job1.setReducerClass(Stage1Reducer.class);

        // Set input/output path & mapper
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path output = new Path(files[1]);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job1, output);

        // task completion
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}
