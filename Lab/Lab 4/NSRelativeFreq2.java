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
 
 
public class NSRelativeFreq2 {
 
    public static class RelativeMapper extends Mapper<Object, Text, StringPair, IntWritable> {
 
        private final static IntWritable one = new IntWritable(1);      
         
        private StringPair wordPair = new StringPair();
         
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
                    wordPair.set(term1, term2);
                    context.write(wordPair, one);
                     
                    //emit one more special key
                    wordPair.set(term1, "*");
                    context.write(wordPair, one);
                }
            }               
        }       
    }
     
    public static class RelFreqCombiner extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {
         
        private IntWritable result = new IntWritable();
         
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
             
            result.set(sum);
            context.write(key, result);
        }       
    }
     
    public static class RelFreqPartitioner extends Partitioner<StringPair, IntWritable>{
         
        public int getPartition(StringPair key, IntWritable value, int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }   
         
    }
 
    //The order is decided by the compareTo() function of StringPair
    public static class RelFreqReducer extends Reducer<StringPair, IntWritable, StringPair, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private double curMarginal = 0;
 
        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
             
            String second = key.getSecond();
             
            if(!second.equals("*")){
                double relFreq = sum/curMarginal;
                result.set(relFreq);
                context.write(key, result);
            }else{
                //for a special key, we only record its value, for further computation
                curMarginal = sum;
            }           
        }
    }
     
     
    public static void main(String[] args) throws Exception {       
         
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nonsymmetric relative frequency v2");
        job.setJarByClass(NSRelativeFreq2.class);
        job.setMapperClass(RelativeMapper.class);
        job.setCombinerClass(RelFreqCombiner.class);
        job.setReducerClass(RelFreqReducer.class);
        //either add this partitioner, or override the hashCode() function in StringPair
        job.setPartitionerClass(RelFreqPartitioner.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(StringPair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setNumReduceTasks(2);       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
