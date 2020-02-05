package analyze.tweet;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final IntWritable          TWEET_SCORE   = new IntWritable(1);
	public static final IntWritable          RETWEET_SCORE = new IntWritable(2);
	public static final IntWritable          MENTION_SCORE = new IntWritable(1);
	public static final IntWritable			 PAIR_SCORE = new IntWritable(1);

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	
	public static class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		//Parse CSVs,
		//Map the user screen name, tweet id, or hashtags used
		// to the relative weight for its appearance
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());
			
			switch(trendingOn)
			{
			case USER:
				word.set(tweet.getUserScreenName());
				
				//(# tweets by user) + 2*(#retweeted) + (#mentioned)
				
				//(#tweetsbyuser)
				context.write(word, TWEET_SCORE);
				
				//(#retweeted)*(retweetscore)
				if (tweet.wasRetweetOfUser())
				{
					word.set(tweet.getRetweetedUser());
					context.write(word, RETWEET_SCORE);
				}
				
				//#mentioned
				for (String s : tweet.getMentionedUsers())
				{
					word.set(s);
					context.write(word, MENTION_SCORE);
				}
				break;
			case TWEET:
				//(1 + 2*#retweeted)
				//need to convert tweetid into string
				String strTweetID = tweet.getId().toString();
				
				word.set(strTweetID);
				context.write(word, TWEET_SCORE);
				
				if (tweet.wasRetweetOfTweet())
				{
					strTweetID = tweet.getRetweetedTweet().toString();
					
					word.set(strTweetID);
					context.write(word, RETWEET_SCORE);
				}
				break;
			case HASHTAG:
				//(#hashtag used)
				for (String ht : tweet.getHashtags())
				{
					word.set(ht);
					context.write(word, TWEET_SCORE);
				}
				break;
			case HASHTAG_PAIR:
				//#tweets of the given pair of hashtags occurs in
				
				//first, generate all pairs of hashtags in the hashtags
				List<String> hashtags = tweet.getHashtags();
				if (hashtags.size() > 1)
				{
					Collections.sort(hashtags);
					for (int i = 0; i < hashtags.size() - 1; i++)
					{
						for (int j = i + 1; j < hashtags.size(); j++)
						{
							word.set("(" + hashtags.get(i) + "," + hashtags.get(j) + ")");
							context.write(word, PAIR_SCORE);
						}
					}
				}
				break;
			}
		}
	}

	public static class PopularityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			//takes output from Mapper & calculates score for each key
			
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));


		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;
		job.setJarByClass(TweetPopularityMR.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(TweetMapper.class);
		job.setReducerClass(PopularityReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
