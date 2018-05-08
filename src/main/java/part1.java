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


//part 1
//rank movie by count(reviews)
//mapreduce on reviews table only? then map movieid?

public class ReviewCount {
 	public static class MapForReviewCount extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		private BufferedReader br;
 		private static HashMap<String, String> movieTitles = new HashMap<String, String>();


 		protected void setup(Context context) throws IOException, InterruptedException {
 			//pass path to movies.csv to loadMoviesHashMap
 		}


 		//open movies.csv and store movie titles in a hash table
 		private void loadMoviesHashMap(Path filePath, Context context)
			throws IOException {

		String strLineRead = "";

		try {
			br = new BufferedReader(new FileReader(filePath.toString()));

			// Read each line, split and load to HashMap
			while ((strLineRead = br.readLine()) != null) {
				String movieFields[] = strLineRead.split(",");
				String movieId = movieFields[0];
				String movieTitle = movieFields[1];
				
				//handle movie titles containing commas
				for(int i = 2; i < movieFields.length - 1; i++) {
					movieTitle = String.join(movieTitle, ",", movieFields[i]);
				}

				movieTitles.put(movieId.trim(), movieTitle.trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (br != null) {
				br.close();
			}
		}


 		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString(); 
			String[] words=line.split(",");
			String movieId = words[1];

			IntWritable outputValue = new IntWritable(1);
			Text outputKey = new Text(movieTitles.get(movieId.trim()));
			con.write(outputKey, outputValue);
		}
	}	
	
	public static class ReduceForReviewCount extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}

	public static void main(String [] args) throws Exception
	{
		// Get input argument and setup configuration
		Configuration config = new Configuration();
		String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

		// setup mapreduce job
		Job job = new Job(config,"bigdata13_part1");
		job.setJarByClass(ReviewCount.class);

		// setup mapper
		job.setMapperClass(MapForReviewCount.class);

		// setup reducer
		job.setReducerClass(ReduceForReviewCount.class);

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



