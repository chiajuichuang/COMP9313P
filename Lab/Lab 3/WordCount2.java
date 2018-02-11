package comp9313.lab3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount2 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private IntWritable count = new IntWritable();
		private Text word = new Text();

		private static HashMap<String, Integer> map = new HashMap<String, Integer>();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			while (itr.hasMoreTokens()) {
				String term = itr.nextToken().toLowerCase();
				if(map.containsKey(term)){
					map.put(term, map.get(term)+1);
				}else{
					map.put(term, 1);
				}				
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<String, Integer> > sets = map.entrySet();
			for(Entry<String, Integer> entry: sets){
				word.set(entry.getKey());
				count.set(entry.getValue());
				context.write(word, count);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count improved");
		job.setJarByClass(WordCount2.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}