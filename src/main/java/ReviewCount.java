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

    public static class Stage1Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
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
            if (cnt > 0) {
                con.write(new IntWritable(cnt), new Text(title));
            }
        }
    }

    public static class SortMapper extends Mapper<LongWritable, Text, IntWritable,
            Text> {
        @Override
        public void map(LongWritable key, Text value, Context con) throws IOException,
                InterruptedException {
            String[] lines = value.toString().split("\n");

            for (String line : lines) {
                String[] parts = line.split("\t");
                con.write(new IntWritable(Integer.parseInt(parts[0])), new Text(parts[1]));
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable cnt, Iterable<Text> values, Context con) throws
                IOException, InterruptedException {
            for (Text value : values) {
                con.write(cnt, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Get input argument and setup configuration
        Configuration config = new Configuration();
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // setup mapreduce job
        Job job1 = new Job(config, "Part 1: review count - Stage 1: Get counts of each movie");
        job1.setJarByClass(ReviewCount.class);
        // setup reducer
        job1.setReducerClass(Stage1Reducer.class);

        // set input/output path & mapper
        String path_base = files[0];
        Path input1 = new Path(path_base + "/movies");
        Path input2 = new Path(path_base + "/reviews");
        Path stage1output = new Path(files[1] + "/stage1");
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, input1, TextInputFormat.class, MoviesMapper.class);
        MultipleInputs.addInputPath(job1, input2, TextInputFormat.class, ReviewsMapper.class);
        FileOutputFormat.setOutputPath(job1, stage1output);

        // task completion
        int code = job1.waitForCompletion(true) ? 0 : 1;

        //sort previous job results
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

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
