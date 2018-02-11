package comp9313.lab4;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 
public class NSRelativeFreq {
 
    public static class RelativeMapper extends Mapper<Object, Text, Text, IntWritable> {
 
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
                    //This is for nonsymmetric computation
                    word.set(term1 + " " + term2);
                    context.write(word, one);
                     
                    //emit one more special key
                    word.set(term1 + " *");
                    context.write(word, one);
                }
            }               
        }       
    }
     
    public static class RelFreqCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
         
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
    
    /*
     * If you use more than 1 reducer, then you need to write a partitioner to guarantee that
     *   all key-value pairs relevant to the first term are sent to the same reducer!
     */
    public static class RelFreqPartitioner extends Partitioner<Text, IntWritable>{
        
        public int getPartition(Text key, IntWritable value, int numPartitions) {
        	
        	//get the first term, and compute the hash value based on it
        	String firstWord = key.toString().split(" ")[0];
            return (firstWord.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }   
         
    }
 
    //The key containing "*" will always arrive at the reducer first     
    public static class RelFreqReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private double curMarginal = 0;
 
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            //no term contains "*", and thus we can use this to detect the special key
            String specialChar= "*";
             
            if(!key.toString().contains(specialChar)){
                double relFreq = sum/curMarginal;
                result.set(relFreq);
                context.write(key, result);
            }else{
                //for a special key, we only record its value for further computation
                curMarginal = sum;
            }           
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nonsymmetric relative frequency");
        job.setJarByClass(NSRelativeFreq.class);
        job.setMapperClass(RelativeMapper.class);
        job.setCombinerClass(RelFreqCombiner.class);
        //job.setPartitionerClass(RelFreqPartitioner.class);
        job.setReducerClass(RelFreqReducer.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        //job.setNumReduceTasks(2);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
