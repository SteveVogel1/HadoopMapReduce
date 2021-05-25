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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class GroupByUmsatz_1 extends Configured implements Tool {

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

		job.setNumReduceTasks(1);
		job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

	
			String[] arr = value.toString().split("\t");
			String store = arr[2].trim();

			DoubleWritable val = new DoubleWritable(Double.parseDouble(arr[4]));
			context.write (new Text(store), val);
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		double sum = 0;
		DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

			for (DoubleWritable val : values){
				sum += val.get();
			}

			result.set(sum);
     		context.write(key, result);
			sum = 0;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new GroupByUmsatz_1(), args);
		System.exit(res);
	}
}
