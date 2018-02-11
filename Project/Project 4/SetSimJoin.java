package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//================================================================================
// Note
//--------------------------------------------------------------------------------
// This assignment has been broken down into two stages, Compute Similarity and Sort. 
// Each stage has its own mapper and reducer and the cleanup function as required. 
// - The driver (main) firstly calls the Compute Similarity job to parse the input
//   file, calculate the similarity and convert to the desired format for the next
//   job. Prefix filtering has been implemented to reduce the number of items needed
//   to process in this job. Load balancing has also been taken care of so that each 
//   partition would have similar sizes. 
// - The intermediate results are stored in a temporary directory.
// - The Sort job removes duplicates and sorts the output according to the assignment
//   specs. The Sort job implements the secondary sorting technique so that the output
//   can be sorted by the first set followed by the second set. In-mapper combining
//   is also used here to reduce the items to be processed in the reducers.
//================================================================================

public class SetSimJoin {
    // ========================================
    // Compute Similarity
    // ========================================
    public static class ComputeSMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable elKey = new IntWritable();
        private Text result = new Text();
        private double threshold;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Init
            threshold = context.getConfiguration().getDouble("THRESHOLD", 0.0);
            String[] items = value.toString().replaceFirst(" ", ",").split(",");
            int rid = Integer.parseInt(items[0]);
            String tokensStr = items[1];
            String[] tokens = tokensStr.toString().split(" ");
            int prefixLen = tokens.length - (int) Math.ceil((double) tokens.length * threshold) + 1;
            
            // Emit
            for (int i = 0; i < prefixLen; ++i) {
                elKey.set(Integer.parseInt(tokens[i]));
                result.set(rid + ":" + tokensStr);
                context.write(elKey, result);
            }
        }
    }

    public static class ComputeSReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> {
        private double threshold;

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Init
            threshold = context.getConfiguration().getDouble("THRESHOLD", 0.0);
            TreeMap<Integer, TreeSet<Integer>> map = new TreeMap<>();

            // Put the items in order
            for (Text val : values) {
                String[] items = val.toString().split(":");
                TreeSet<Integer> set = new TreeSet<Integer>();
                String[] tokens = items[1].split(" ");
                for (String token : tokens) {
                    set.add(Integer.parseInt(token));
                }
                map.put(Integer.parseInt(items[0]), set);
            }

            // Compute similarity and emit
            for (int i : map.keySet()) {
                for (int j : map.keySet()) {
                    if (i != j && i < j) {
                        TreeSet<Integer> union = new TreeSet<Integer>(map.get(i));
                        union.addAll(map.get(j));

                        TreeSet<Integer> intersection = new TreeSet<Integer>(map.get(i));
                        intersection.retainAll(map.get(j));

                        double s = (double) intersection.size() / (double) union.size();
                        if (s >= threshold)
                            context.write(new Text(i + "\t" + j), new DoubleWritable(s));
                    }
                }
            }
        }
    }

    public static class ComputeSPartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable key, Text val, int numPartitions) {
            int hash = key.hashCode();
            int partition = hash % numPartitions;
            return partition;
        }
    }

    // ========================================
    // Sort
    // ========================================
    public static class SimilarityKey implements WritableComparable<SimilarityKey> {
        public int first_;
        public int second_;

        public SimilarityKey() {
        }

        public SimilarityKey(int first, int second) {
            super();
            set(first, second);
        }

        public void set(int first, int second) {
            first_ = first;
            second_ = second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first_);
            out.writeInt(second_);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first_ = in.readInt();
            second_ = in.readInt();
        }

        @Override
        public int compareTo(SimilarityKey o) {
            int firstCmp = Integer.compare(first_, o.first_);
            if (firstCmp != 0)
                return firstCmp;
            else
                return Integer.compare(second_, o.second_);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SimilarityKey))
                return false;
            if (obj == this)
                return true;
            return (compareTo((SimilarityKey) obj) == 0);
        }

        @Override
        public int hashCode() {
            return (first_ + ":" + second_).hashCode();
        }
    }

    public static class SortMapper extends Mapper<Object, Text, SimilarityKey, DoubleWritable> {
        private HashMap<SimilarityKey, Double> map = new HashMap<>();
        private DoubleWritable result = new DoubleWritable();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Init
            String[] items = value.toString().split("\t");
            SimilarityKey simKey = new SimilarityKey();

            // In-mapper combining
            simKey.set(Integer.parseInt(items[0]), Integer.parseInt(items[1]));
            if (!map.containsKey(simKey))
                map.put(simKey, Double.parseDouble(items[2]));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emit
            for (Entry<SimilarityKey, Double> entry : map.entrySet()) {
                result.set(entry.getValue());
                context.write(entry.getKey(), result);
            }
        }
    }

    public static class SortReducer extends Reducer<SimilarityKey, DoubleWritable, Text, DoubleWritable> {
        private Text sets = new Text();

        @Override
        public void reduce(SimilarityKey key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            // Emit
            sets.set("(" + key.first_ + "," + key.second_ + ")");
            for(DoubleWritable w : values) {
                context.write(sets, w);
                break;
            }
        }
    }

    public static class SortPartitioner extends Partitioner<SimilarityKey, DoubleWritable> {
        @Override
        public int getPartition(SimilarityKey key, DoubleWritable val, int numPartitions) {
            int hash = key.first_;
            int partition = Math.abs(hash) % numPartitions;
            return partition;
        }
    }

    public static void main(String[] args) throws Exception {
        // Init
        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        String tmpDir = args[1] + "/../tmp_" + args[2] + "_" + args[3] + "_" + timeStamp;
        int reducers = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        conf.setDouble("THRESHOLD", Double.parseDouble(args[2]));

        // ========================================
        // Compute Similarity
        // ========================================
        Job job = Job.getInstance(conf, "Compute Similarity");
        job.setNumReduceTasks(reducers);
        job.setJarByClass(SetSimJoin.class);
        job.setMapperClass(ComputeSMapper.class);
        job.setReducerClass(ComputeSReducer.class);
        job.setPartitionerClass(ComputeSPartitioner.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tmpDir));
        job.waitForCompletion(true);

        // ========================================
        // Sort
        // ========================================
        Job sortJob = Job.getInstance(conf, "Sort");
        sortJob.setNumReduceTasks(reducers);
        sortJob.setJarByClass(SetSimJoin.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setPartitionerClass(SortPartitioner.class);
        sortJob.setOutputKeyClass(SimilarityKey.class);
        sortJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(sortJob, new Path(tmpDir));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
        sortJob.waitForCompletion(true);
    }
}
