package comp9313.lab4;

import java.io.IOException;
import java.util.ArrayList;
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


public class CoTermSynPair {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			
			ArrayList<String> termArray = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				termArray.add(itr.nextToken().toLowerCase());
			}
			for(int i=0;i<termArray.size();i++){
				String term1 = termArray.get(i);
				for(int j=i+1;j<termArray.size();j++){
					String term2 = termArray.get(j);					
					if(term1.compareTo(term2) < 0){
						word.set(term1 + " " + term2);						
					}else{
						word.set(term2 + " " + term1);
					}					
					context.write(word, one);
				}
			}				
		}		
	}

	public static class SynPairReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		Job job = Job.getInstance(conf, "term co-occurrence symmetric pair");
		job.setJarByClass(CoTermSynPair.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SynPairReducer.class);
		job.setReducerClass(SynPairReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
