package comp9313.lab5;
 
//import ...
 
public class BooleanInvertedList {
     
    //Implement your own StringPair or TextPair class
    public static class BILMapper extends Mapper<Object, Text, StringPair, Text> { 
        
	// ... initialize some variables here if necessary
         
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");             
            
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	    // ... do your map job here	
        }       
    }
     
    public static class BILPartitioner extends Partitioner<StringPair, Text>{
        
	// ... override the getPartition() function here
    }
    
    public static class BILGroupingComparator extends WritableComparator{
    	
    	protected BILGroupingComparator(){
    		super(StringPair.class, true);
    	}
    	
	// ... override the compare() function here
    }
    
    public static class BILReducer extends Reducer<StringPair, Text, Text, Text> {
  
         public void reduce(StringPair key, Iterable<Text> values, Context context)
                 throws IOException, InterruptedException {
        	     		
        	 // ... do your reduce job here
         }
    }
     
     
    public static void main(String[] args) throws Exception {       
         
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "BIL");
        job.setJarByClass(BooleanInvertedList.class);
        job.setMapperClass(BILMapper.class);
        job.setReducerClass(BILReducer.class);
        //either add this partitioner, or override the hashCode() function in StringPair
        job.setPartitionerClass(BILPartitioner.class);
        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(BILGroupingComparator.class);        
        job.setNumReduceTasks(2);       
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

