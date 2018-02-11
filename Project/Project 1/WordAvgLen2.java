/**************************************************
* COMP9313 Project 1
* Chia-Jui Chuang (5095972)
* WordAvgLen2
**************************************************/

package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordAvgLen2 {

    //==========================================================================
    // Pair
    // - Class to store a count and a sum
    // - Implements Writable
    // - Supports getters and setters for count and sum
    // - Supports addition for count and sum
    //==========================================================================
    public static class Pair implements Writable {
        private int count_;
        private int sum_;

        public Pair() {
            set(0, 0);
        };

        public Pair(int count, int sum) {
            set(count, sum);
        }

        public void set(int count, int sum) {
            count_ = count;
            sum_ = sum;
        }

        public void set(Pair rhs) {
            count_ = rhs.getCount();
            sum_ = rhs.getSum();
        }

        public int getCount() {
            return count_;
        }

        public int getSum() {
            return sum_;
        }

        public void addCount(int n) {
            count_ += n;
        }

        public void addSum(int n) {
            sum_ += n;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(count_);
            out.writeInt(sum_);
        }

        public void readFields(DataInput in) throws IOException {
            count_ = in.readInt();
            sum_ = in.readInt();
        }
    }

    //==========================================================================
    // AvgLenMapper
    // - The Mapper Class
    // - Extracts tokens from the input
    // - Computes the length of the tokens that start with [a-z]
    // - Adds and stores the intermediate results in a HashMap accordingly.
    //   Format: <[Alphabet], [Pair([Count], [Sum])]>
    // - Iterates the HashMap in 'cleanup' and emits the alphabet with the aggregated pair.
    //==========================================================================
    public static class AvgLenMapper extends Mapper<Object, Text, Text, Pair> {
        private static HashMap<String, Pair> map = new HashMap<>();
        private Pair pair = new Pair();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase();

                if (token.length() > 0 && token.charAt(0) >= 'a' && token.charAt(0) <= 'z') {
                    String charKey = token.substring(0, 1);

                    if (map.containsKey(charKey)) {
                        Pair pPair = map.get(charKey);
                        pPair.addCount(1);
                        pPair.addSum(token.length());
                    } else {
                        map.put(charKey, new Pair(1, token.length()));
                    }
                }
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Pair> entry : map.entrySet()) {
                word.set(entry.getKey());
                pair.set(entry.getValue());

                context.write(word, pair);
            }
        }
    }

    //==========================================================================
    // AvgLenReducer
    // - The Reducer Class
    // - Calculates the average length with the input pairs
    // - Writes to output
    //==========================================================================
    public static class AvgLenReducer extends Reducer<Text, Pair, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;

            for (Pair val : values) {
                count += val.getCount();
                sum += val.getSum();
            }
            double avg = sum / count;

            result.set(avg);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word average length");
        job.setJarByClass(WordAvgLen2.class);
        job.setMapperClass(AvgLenMapper.class);
        job.setReducerClass(AvgLenReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Pair.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
