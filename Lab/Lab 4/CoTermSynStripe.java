package comp9313.lab4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CoTermSynStripe {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");			
						
			ArrayList<String> termArray = new ArrayList<String>();
			while (itr.hasMoreTokens()) {
				termArray.add(itr.nextToken().toLowerCase());
			}
			
			for(int i=0;i<termArray.size();i++){
				MapWritable record = new MapWritable();
				Text word1 = new Text(termArray.get(i));
				
				for(int j=i+1;j<termArray.size();j++){
					Text word2 = new Text(termArray.get(j));
					
					//consider the order of two words!
					if(termArray.get(i).compareTo(termArray.get(j)) < 0){
						if(record.containsKey(word2)){
							IntWritable count = (IntWritable)record.get(word2);
							count.set(count.get() + 1);
							record.put(word2, count);
						}else{
							record.put(word2, new IntWritable(1));
						}
					}else{
						MapWritable ttmap = new MapWritable();
						ttmap.put(word1, new IntWritable(1));
						context.write(word2, ttmap);
					}
				}
				context.write(word1, record); 
			}
		}		
	}

	public static class SynStripeCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
		
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {	
			
			MapWritable map = new MapWritable();
			
			for (MapWritable val : values) {
				Set<Entry<Writable, Writable> > sets = val.entrySet();
				for(Entry<Writable, Writable> entry: sets){
					Text word2 = (Text)entry.getKey();
					int count = ((IntWritable)entry.getValue()).get();
					if(map.containsKey(word2)){
						map.put(word2, new IntWritable(((IntWritable)map.get(word2)).get() + count));
					}else{
						map.put(word2, new IntWritable(count));
					}						
				}
			}
			context.write(key, map);
		}
	}
	
	public static class SynStripeReducer extends Reducer<Text, MapWritable, Text, IntWritable> {

		private Text word = new Text();
		private IntWritable count = new IntWritable();

		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String, Integer> map = new HashMap<String, Integer>();

			for (MapWritable val : values) {
				Set<Entry<Writable, Writable>> sets = val.entrySet();
				for (Entry<Writable, Writable> entry : sets) {
					String word2 = ((Text) entry.getKey()).toString();
					int num = ((IntWritable) entry.getValue()).get();
					if (map.containsKey(word2)) {
						map.put(word2, map.get(word2).intValue() + num);
					} else {
						map.put(word2, new Integer(num));
					}
				}
			}

			// Do sorting, in order to make the result exactly the same as the pair approach
			Object[] sortkey = map.keySet().toArray();
			Arrays.sort(sortkey);
			for (int i = 0; i < sortkey.length; i++) {
				word.set(key.toString() + " " + sortkey[i].toString());
				count.set(map.get(sortkey[i]));
				context.write(word, count);
			}	
			
			//This does not do sort
			/*Set<Entry<String, Integer> > sets = map.entrySet();
			for(Entry<String, Integer> entry: sets){
				word.set(key.toString() + " " + entry.getKey());
				count.set(entry.getValue());
				context.write(word, count);
			}*/
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "term co-occurrence symmetric stripe");
		job.setJarByClass(CoTermSynStripe.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(SynStripeCombiner.class);
		job.setReducerClass(SynStripeReducer.class);		
		job.setMapOutputValueClass(MapWritable.class);		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

