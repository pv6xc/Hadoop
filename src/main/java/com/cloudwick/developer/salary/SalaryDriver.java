package com.cloudwick.developer.salary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalaryDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}
		Job job = new Job(getConf());
		job.setJarByClass(SalaryDriver.class);
		job.setJobName(this.getClass().getName());
		job.setMapperClass(SalaryMapper.class);
		job.setCombinerClass(SalaryReducer.class);
		job.setReducerClass(SalaryReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		if (job.waitForCompletion(true)) {
			return 0;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		// delete output if exists
		FileSystem f = FileSystem.get(new Configuration());
		f.delete(new Path("output"), true);
		
		int exitCode = ToolRunner.run(new SalaryDriver(), args);
		System.exit(exitCode);
	}
}
