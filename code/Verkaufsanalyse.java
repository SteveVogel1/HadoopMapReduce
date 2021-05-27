package team1MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.BinaryPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.io.IOException;

public class Verkaufsanalyse extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <in> <out>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;

        }

        Job job = Job.getInstance(getConf(), "minimapredext");
        job.setJarByClass(this.getClass());

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(AnalyseMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(CustomPartitioner.class);

        job.setNumReduceTasks(4); // If > 14, some reducers will be idle
        job.setReducerClass(AnalyseReducer.class);

        job.setOutputKeyClass(CustomLongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Splits each line using \t as delimiter and extracts time(hour) and amount.
     * Outputs Longwritable with hour as key and DoubleWritable with amount as value.
     */
    public static class AnalyseMapper extends Mapper<LongWritable, Text, LongWritable, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] arr = value.toString().split("\t");
            String[] time = arr[1].split(":");

            LongWritable k = new LongWritable(Long.parseLong(time[0]));

            DoubleWritable val = new DoubleWritable(Double.parseDouble(arr[4]));
            context.write(k, val);
        }
    }

    /**
     * Overrides toString so that we get a nicer output.
     */
    public static class CustomLongWritable extends LongWritable {

        public CustomLongWritable(long value) {
            super(value);
        }

        @Override
        public String toString() {
            return "Stunde: " + get();
        }

    }

    /**
     * Maps the output of the mapper to partitions based on the key(hour).
     * Guarantees proper ordering of the output over all output files.
     */
    public static class CustomPartitioner extends Partitioner<LongWritable, DoubleWritable> {
        @Override
        public int getPartition(LongWritable key, DoubleWritable value, int numPartitions) {
            // We assume all purchases are between 6 and 20
            // if we subtract 6, the max value is 14
            int keySubtracted = (int) key.get() - 6;
            int keyPerPartition = (int) Math.ceil(14 / (double) numPartitions);
            return (int) (keySubtracted / keyPerPartition);
        }
    }

    /**
     * Sums up the amounts for all keys.
     * Outputs the hour as key and the average of all sales for that specific hour as the value.
     */
    public static class AnalyseReducer extends Reducer<LongWritable, DoubleWritable, CustomLongWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double avg = 0;
            int count = 0;
            for (DoubleWritable i : values) {
                avg += i.get();
                count++;
            }
            avg /= count;
            DecimalFormatSymbols localeFormatter = new DecimalFormatSymbols();
            localeFormatter.setGroupingSeparator('\'');
            DecimalFormat formatter = new DecimalFormat("###,###,###.00", localeFormatter);
            Text t = new Text(formatter.format(avg));

            context.write(new CustomLongWritable(key.get()), t);
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Verkaufsanalyse(), args);
        System.exit(res);
    }
}
