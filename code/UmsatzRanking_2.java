package team1MapReduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.io.IOException;
import java.io.*;
import java.util.*;

public class UmsatzRanking_2 extends Configured implements Tool {

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
        job.setMapperClass(RankingMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(HashPartitioner.class);

        job.setNumReduceTasks(1); // cannot have more than 1!
        job.setReducerClass(RankingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * StoreRevenuePair is used for sorting the sales by value.
     * Holds a string with the store name and a double for the total revenue.
     */
    public static class StoreRevenuePair implements Comparable<StoreRevenuePair> {
        public final String store;
        public final Double revenue;

        public StoreRevenuePair(Double revenue, String store) {
            this.store = store;
            this.revenue = revenue;
        }

        /**
         * Used for insertion in the PriorityQueue.
         */
        @Override
        public int compareTo(StoreRevenuePair other) {
            double diff = this.revenue - other.revenue;
            if (diff < 0) {
                return -1;
            } else if (diff > 0) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((revenue == null) ? 0 : revenue.hashCode());
            result = prime * result + ((store == null) ? 0 : store.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StoreRevenuePair other = (StoreRevenuePair) obj;
            if (revenue == null) {
                if (other.revenue != null)
                    return false;
            } else if (!revenue.equals(other.revenue))
                return false;
            if (store == null) {
                if (other.store != null)
                    return false;
            } else if (!store.equals(other.store))
                return false;
            return true;
        }
    }

    /**
     * Splits each line using \t as delimiter and extracts store and amount.
     * Creates a StoreRevenuePair for each processed line and stores it in the PriorityQueue retaining
     * the largest 10 values.
     */
    public static class RankingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private PriorityQueue<StoreRevenuePair> intermediateRanking;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            intermediateRanking = new PriorityQueue<StoreRevenuePair>();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String store = tokens[0];
            double revenue = Double.parseDouble(tokens[1]);

            intermediateRanking.add(new StoreRevenuePair(revenue, store));

            if (intermediateRanking.size() > 10) { // keeps the priority queue at 10 elements
                intermediateRanking.poll();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            while (!intermediateRanking.isEmpty()) {
                StoreRevenuePair element = intermediateRanking.poll();
                context.write(new Text(element.store), new DoubleWritable(element.revenue));
            }
        }
    }

    /**
     * Creates a new PriorityQueue and keeps the then largest values over all stores.
     */
    public static class RankingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private PriorityQueue<StoreRevenuePair> ranking;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            ranking = new PriorityQueue<StoreRevenuePair>();
        }

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
            for (DoubleWritable w : value) {
                ranking.add(new StoreRevenuePair(w.get(), key.toString()));
            }

            if (ranking.size() > 10) {
                ranking.poll();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            DecimalFormatSymbols localeFormatter = new DecimalFormatSymbols();
            localeFormatter.setGroupingSeparator('\'');
            DecimalFormat formatter = new DecimalFormat("###,###,###.00", localeFormatter);

            ArrayList<StoreRevenuePair> reversed = new ArrayList<StoreRevenuePair>(); // Needed get the 10 largest values
            while (!ranking.isEmpty()) {
                reversed.add(ranking.poll());
            }

            for (int i = reversed.size() - 1; i >= 0; i--) { // traverse backwards
                Text t = new Text(formatter.format(reversed.get(i).revenue));
                context.write(new Text(reversed.get(i).store), t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UmsatzRanking_2(), args);
        System.exit(res);
    }
}
