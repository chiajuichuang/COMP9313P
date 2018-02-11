package comp9313.ass2;

//import ...

public class SingleSourceSP {


    public static String OUT = "output";
    public static String IN = "input";

    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text> {

        @Override
       public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // YOUR JOB: map function
            // ... ...
        }

    }


    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // YOUR JOB: reduce function
            // ... ...
        }
    }


    public static void main(String[] args) throws Exception {        

        IN = args[0];

        OUT = args[1];

        int iteration = 0;

        String input = IN;

        String output = OUT + iteration;

	    // YOUR JOB: Convert the input file to the desired format for iteration, i.e., 
        //           create the adjacency list and initialize the distances
        // ... ...

        boolean isdone = false;

        while (isdone == false) {

            // YOUR JOB: Configure and run the MapReduce job
            // ... ...                   
            
            input = output;    

            iteration ++;       

            output = OUT + iteration;

            //You can consider to delete the output folder in the previous iteration to save disk space.

            // YOUR JOB: Check the termination criterion by utilizing the counter
            // ... ...

            if(the termination condition is reached){
                isdone = true;
            }
        }

        // YOUR JOB: Extract the final result using another MapReduce job with only 1 reducer, and store the results in HDFS
        // ... ...
    }

}

