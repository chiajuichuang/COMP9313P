package comp9313.lab5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BooleanInvertedList2 {

	public static class BILMapper extends Mapper<Object, Text, StringPair, Text> {

		private StringPair pair = new StringPair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			while (itr.hasMoreTokens()) {
				pair.set(itr.nextToken().toLowerCase(), fileName);
				context.write(pair, new Text(fileName));
			}
		}
	}

	public static class BILPartitioner extends Partitioner<StringPair, Text> {

		public int getPartition(StringPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class BILGroupingComparator extends WritableComparator {

		protected BILGroupingComparator() {
			super(StringPair.class, true);
		}

		public int compare(WritableComparable wc1, WritableComparable wc2) {

			StringPair pair = (StringPair) wc1;
			StringPair pair2 = (StringPair) wc2;

			return pair.getFirst().compareTo(pair2.getFirst());
		}
	}
	
	public static class BILReducer extends Reducer<StringPair, Text, Text, StringArrayWritable> {
		public void reduce(StringPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> fnArray = new ArrayList<String>();
			
        	 Set<String> files = new HashSet<String>();
        	 for(Text fName : values){
        		 String name = fName.toString();
        		 if(!files.contains(name)){        			 
        			 fnArray.add(name);
        			 files.add(name);
        		 }
        	 }  
        	 StringArrayWritable array = new StringArrayWritable(fnArray);
        	 context.write(new Text(key.getFirst()), array);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "nonsymmetric relative frequency v2");
		job.setJarByClass(BooleanInvertedList2.class);
		job.setMapperClass(BILMapper.class);
		job.setReducerClass(BILReducer.class);
		// either add this partitioner, or override the hashCode() function in StringPair
		job.setPartitionerClass(BILPartitioner.class);
		job.setMapOutputKeyClass(StringPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(StringArrayWritable.class);
		
		job.setGroupingComparatorClass(BILGroupingComparator.class);
		
		job.setNumReduceTasks(2);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
