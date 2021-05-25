package team1MapReduce;

//== MinimalMapReduceExt: The simplest possible MapReduce driver, which shows the defaults and uses own Mapper and Reducer

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//== these imports are new
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

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
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setPartitionerClass(HashPartitioner.class);

		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		private TreeMap<Double, String> tmap;
	
		@Override
		public void setup(Context context) throws IOException, InterruptedException
		{
			tmap = new TreeMap<Double, String>(Collections.reverseOrder());
		}
	
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] tokens = value.toString().split("\t");
			String store = tokens[0];
			double revenue = Double.parseDouble(tokens[1]);
	
			tmap.put(revenue, store);

			if (tmap.size() > 10)
			{
				tmap.remove(tmap.lastKey());
			}
		}
	
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for (Map.Entry<Double, String> entry : tmap.entrySet()) 
			{
				double revenue = entry.getKey();
				String store = entry.getValue();
				context.write(new Text(store), new DoubleWritable(revenue));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new UmsatzRanking_2(), args);
		System.exit(res);
	}
}
