package com.cloudwick.developer.avgwordcount;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.printf(
					"Usage: %s [generic options] <input dir> <output dir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		// job config declaration
		Job job = new Job(getConf());
		job.setJarByClass(WordCountDriver.class);
		job.setJobName(this.getClass().getName());
		job.setMapperClass(WordCountMapper.class);
		//job.setCombinerClass(WordCountReducer.class);
		//job.setReducerClass(WordCountReducer.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(FloatWritable.class);
		// file i/o paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (job.waitForCompletion(true)) {
			return 0;
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		//delete output if exists
		FileSystem f = FileSystem.get(new Configuration());
		f.delete(new Path("output"), true);
		
		int exitCode = ToolRunner.run(new WordCountDriver(), args);
		System.exit(exitCode);
	}
}