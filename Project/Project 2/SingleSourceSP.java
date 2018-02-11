package comp9313.ass2;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// ================================================================================
// Note
// --------------------------------------------------------------------------------
// The assignment has been broken down into three stages, Parse, Core and Output. 
// Each stage has its own mapper and reducer and the cleanup function as required. 
// The driver (main) firstly calls the Parse job to parse the input file and outputs
// the results into a temporary directory.
// The core job then reads the file and starts looking for the shortest paths. The 
// result of each iteration is also stored in the temporary directory. It keeps 
// iterating until there is no update on shortest paths in that iteration.
// Once all the required iterations are complete, the output job is called to format 
// the output to what's specified in the assignment specs.
// --------------------------------------------------------------------------------
// Format
// --------------------------------------------------------------------------------
// [Key] | [Distance] | [Adj List] | [D|P] | [From]
// [Key]      : Key of the node
// [Distance] : Current shortest distance of the node
// [Adj List] : Adjacent list of the node
// [D|P]      : D: Done    - No further processing of the node required
//              P: Pending - Requires further processing of the node
// [From]     : Back pointer to the node that results in the shortest path
// --------------------------------------------------------------------------------
// Output
// --------------------------------------------------------------------------------
// Although it has been said on the course web site that the precision of the final 
// output does not matter, I have rounded the values to #.# to compare my output
// with the sample output.
// ================================================================================

public class SingleSourceSP {

    public static String OUT = "output";
    public static String IN = "input";
    public static String QueryNode = "0";

    public static enum COUNTER {
        UPDATE
    };

    private static class Node {
        private String name_ = "";
        private String weight_ = "";

        public Node(String name, String weight) {
            name_ = name;
            weight_ = weight;
        }

        public String getName() {
            return name_;
        }

        public String getWeight() {
            return weight_;
        }
    }

    // ========================================
    // Parse
    // ========================================
    public static class ParseMapper extends Mapper<Object, Text, LongWritable, Text> {
        HashMap<String, ArrayList<Node>> map = new HashMap<>();
        private LongWritable node = new LongWritable();
        private Text result = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length == 4) {
                if (map.get(tokens[1]) == null) {
                    map.put(tokens[1], new ArrayList<Node>());
                }
                map.get(tokens[1]).add(new Node(tokens[2], tokens[3]));
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Entry<String, ArrayList<Node>> entry : map.entrySet()) {
                String str = entry.getKey() + "|";
                if (entry.getKey().equals(QueryNode))
                    str += "0.0|";
                else
                    str += "-1.0|";

                for (Node node : entry.getValue()) {
                    str += node.getName() + ":" + node.getWeight() + " ";
                }
                str += "|P|-";

                node.set(Long.parseLong(entry.getKey()));
                result.set(str);

                context.write(node, result);
            }
        }
    }

    public static class ParseReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    // ========================================
    // Core
    // ========================================
    public static class SSSPMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable node = new LongWritable();
        private Text result = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String adjList = value.toString().split("\t")[1];
            String tokens[] = adjList.split("\\|");

            node.set(Long.parseLong(tokens[0]));
            result.set(adjList);
            context.write(node, result);

            if (!tokens[1].equals("-1.0") && !tokens[3].equals("D") && !tokens[2].equals("-")) {
                String[] toNodes = tokens[2].split(" ");
                for (String toNode : toNodes) {
                    if (!toNode.isEmpty()) {
                        String[] params = toNode.split(":");
                        node.set(Long.parseLong(params[0]));
                        double tmp = Double.parseDouble(adjList.split("\\|")[1]) + Double.parseDouble(params[1]);
                        result.set(tokens[0] + ":" + Double.toString(tmp));
                        context.write(node, result);
                    }
                }
            }
        }
    }

    public static class SSSPReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Look for the adjacency list
            String adjList = "";
            ArrayList<String> dists = new ArrayList<>();
            for (Text val : values) {
                String valStr = val.toString();
                if (valStr.indexOf("|") != -1)
                    adjList = valStr;
                else
                    dists.add(valStr);
            }

            if (adjList.isEmpty()) {
                adjList = key.toString() + "|-1.0|-|P|-";
            }

            // Get current minimum (double)
            double currMin;
            String currMinStr = adjList.split("\\|")[1];
            if (currMinStr.equals("-1.0"))
                currMin = Double.POSITIVE_INFINITY;
            else
                currMin = Double.parseDouble(currMinStr);

            // Find new minimum (double)
            String[] params = adjList.split("\\|");

            String fromNode = params[0];
            String edges = params[2];
            String currFrom = params[4];

            double newMin = currMin;
            String newFrom = currFrom;
            boolean updated = false;
            for (String dist : dists) {
                String[] pair = dist.split(":");
                String from = pair[0];
                double val = Double.parseDouble(pair[1]);
                if (val < newMin) {
                    newMin = val;
                    newFrom = from;
                    updated = true;
                    context.getCounter(COUNTER.UPDATE).increment(1);
                }
            }

            // Emit result
            String output = "";
            String minStr = (newMin == Double.POSITIVE_INFINITY ? "-1.0" : Double.toString(newMin));
            output += (fromNode + "|");
            output += (minStr + "|");
            output += (edges + "|");
            if (!updated && !minStr.equals("-1.0"))
                output += "D|";
            else
                output += "P|";
            output += newFrom;

            context.write(key, new Text(output));
        }
    }

    // ========================================
    // Output
    // ========================================
    public static class OutputMapper extends Mapper<Object, Text, LongWritable, Text> {
        private LongWritable node = new LongWritable();
        private Text result = new Text();
        private HashMap<String, String> lastMap = new HashMap<>();
        private HashMap<String, String> pathMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t")[1].split("\\|");
            lastMap.put(tokens[0], tokens[4]);
            pathMap.put(tokens[0], tokens[1]);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Entry<String, String> entry : pathMap.entrySet()) {
                String key = entry.getKey();

                Stack<String> stack = new Stack<>();
                String currNode = key;
                while (!currNode.equals("-")) {
                    stack.push(currNode);
                    currNode = lastMap.get(currNode);
                }

                String output = pathMap.get(key) + ":";
                while (!stack.isEmpty()) {
                    output += stack.pop();

                    if (!stack.isEmpty())
                        output += "->";
                }

                node.set(Long.parseLong(key));
                result.set(output);
                context.write(node, result);
            }
        }
    }

    public static class OutputReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] tokens = val.toString().split(":");
                DecimalFormat formatted = new DecimalFormat("#.#");
                double min = Double.valueOf(formatted.format(Double.parseDouble(tokens[0])));
                String output = min + "\t" + tokens[1];
                if (!tokens[0].equals("-1.0"))
                    context.write(key, new Text(output));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        IN = args[0];
        OUT = args[1];
        QueryNode = args[2];
        int iteration = 0;

        String tmpInput = IN;
        String tmpOutput = OUT + "/../tmp/" + iteration++;

        Configuration conf = new Configuration();

        // ========================================
        // Parse Input
        // ========================================
        Job parseJob = Job.getInstance(conf, "Parse Input");
        parseJob.setJarByClass(SingleSourceSP.class);
        parseJob.setMapperClass(ParseMapper.class);
        parseJob.setReducerClass(ParseReducer.class);
        parseJob.setOutputKeyClass(LongWritable.class);
        parseJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(parseJob, new Path(tmpInput));
        FileOutputFormat.setOutputPath(parseJob, new Path(tmpOutput));

        parseJob.waitForCompletion(true);

        // ========================================
        // Core
        // ========================================
        boolean isdone = false;

        while (isdone == false) {
            tmpInput = tmpOutput;
            tmpOutput = OUT + "/../tmp/" + iteration++;

            Job job = Job.getInstance(conf, "Single Source SP");
            job.setJarByClass(SingleSourceSP.class);
            job.setMapperClass(SSSPMapper.class);
            job.setReducerClass(SSSPReducer.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(tmpInput));
            FileOutputFormat.setOutputPath(job, new Path(tmpOutput));

            job.waitForCompletion(true);

            Counters counters = job.getCounters();
            long updates = counters.findCounter(COUNTER.UPDATE).getValue();
            System.out.println("Updates: " + updates);

            if (updates == 0)
                isdone = true;
        }

        // ========================================
        // Format Output
        // ========================================
        tmpInput = tmpOutput;
        tmpOutput = OUT;

        Job job = Job.getInstance(conf, "Format Output");
        job.setJarByClass(SingleSourceSP.class);
        job.setMapperClass(OutputMapper.class);
        job.setReducerClass(OutputReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(tmpInput));
        FileOutputFormat.setOutputPath(job, new Path(tmpOutput));

        job.waitForCompletion(true);

        // ========================================
        // Delete temp files
        // ========================================
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
        // delete existing directory
        Path tmpDir = new Path("hdfs://localhost:9000/user/comp9313/tmp");
        if (fs.exists(tmpDir))
            fs.delete(tmpDir, true);
    }
}
