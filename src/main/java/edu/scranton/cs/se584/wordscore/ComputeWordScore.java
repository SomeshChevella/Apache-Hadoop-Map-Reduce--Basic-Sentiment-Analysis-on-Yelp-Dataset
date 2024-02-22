package edu.scranton.cs.se584.wordscore;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.commons.configuration2.io.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A MapReduce job that performs basic sentiment analysis using Yelp reviews.
 * <p>
 * For each word that appears in the text of a review, the job computes a score
 * summarizing the "positivity" or "negativity" of that word.
 * <p>
 * The score for a word is computed by applying the following rules:
 * <ul>
 * <li>Each occurrence in a 5-star review increases the word's score by 2.
 * <li>Each occurrence in a 4-star review increases the word's score by 1.
 * <li>3-star reviews have no impact on word score.
 * <li>Each occurrence in a 2-star review decreases the word's score by 1.
 * <li>Each occurrence in a 1-star review decreases the word's score by 2.
 * </ul>
 * <p>
 * The final score for a word is the net effect of applying the above rules to
 * every review in the input dataset. The idea is that more positive words will
 * appear more prevalently in the positive (5-star and 4-star) reviews and more
 * negative words more prevalently in the negative (2-star and 1-star) reviews.
 * More neutral words should appear in both positive and negative reviews,
 * effectively "canceling out".
 * <p>
 * The output of the job is a single, tab-delimited text file in the specified
 * output directory that contains each word and its score in descending order by
 * score. Words with equal score may appear in any order.
 * <p>
 * TODO(student) Document any improvements that you have made to the algorithm.
 *
 * @author Daniel M. Jackowitz
 * @author Chevella V Someshwar.
 */
public class ComputeWordScore extends Configured implements Tool {

	public static class ComputeWordScoreMapper extends Mapper<Object, Text, Text, LongWritable> {
		private static final LongWritable ONE = new LongWritable(1);
		private static final LongWritable TWO = new LongWritable(2);
		private static final LongWritable ZERO = new LongWritable(0);
		private static final LongWritable NEGATIVE_ONE = new LongWritable(-1);
		private static final LongWritable NEGATIVE_TWO = new LongWritable(-2);
		private LongWritable score;
		private Text word = new Text();
		private final JSONParser parser = new JSONParser();

		private JSONObject parse(final String line) {
			try {
				return (JSONObject) parser.parse(line);
			} catch (ParseException exception) {
				throw new RuntimeException(exception);
			}
		}

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final JSONObject review = parse(value.toString());
			int rating = ((Number) review.get("stars")).intValue();

			if (rating > 3) {
				score = (rating == 4) ? ONE : TWO;
			} else if (rating < 3) {
				score = (rating == 2) ? NEGATIVE_ONE : NEGATIVE_TWO;
			} else {
				score = ZERO;
			}
			StringTokenizer itr = new StringTokenizer(review.get("text").toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, score);
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class SortMapper extends Mapper<Object, Text, LongWritable, Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			LongWritable num = new LongWritable(Integer.valueOf(str[1]));
			Text word = new Text(str[0]);
			context.write(num, word);
		}
	}

	public static class SortReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	@Override
	public int run(String[] arguments) throws Exception {
		if (arguments.length < 2) {
			System.err.println("Usage: <input> <output>");
			return 2; // Return 2 to signal a problem with the arguments.
		}
		Path tempDir = new Path("temp" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "score words based on rating");
		job.setJarByClass(ComputeWordScore.class);
		FileInputFormat.addInputPath(job, new Path(arguments[0]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, tempDir);
		job.setMapperClass(ComputeWordScoreMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.waitForCompletion(true);
		final Configuration conf2 = getConf();
		final Job sortJob = Job.getInstance(conf2, "Sort the words");
		sortJob.setJarByClass(ComputeWordScore.class);
		FileInputFormat.addInputPath(sortJob, tempDir);
		sortJob.setMapOutputKeyClass(LongWritable.class);
		sortJob.setMapOutputValueClass(Text.class);
		sortJob.setMapperClass(SortMapper.class); 
		
		sortJob.setInputFormatClass(TextInputFormat.class); // ask //added
		sortJob.setSortComparatorClass(LongWritable.DecreasingComparator.class); //added
		
		sortJob.setCombinerClass(SortReducer.class);
		sortJob.setReducerClass(SortReducer.class);
		sortJob.setOutputKeyClass(LongWritable.class);
		sortJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(sortJob, new Path(arguments[1]));
		sortJob.waitForCompletion(true);
		 
	    
		return sortJob.waitForCompletion(true) ? 0 : 1; // Return 0 on success, 1 on failure.s
	}

	public static void main(String[] arguments) throws Exception {
		System.exit(ToolRunner.run(new ComputeWordScore(), arguments));
	}
}
