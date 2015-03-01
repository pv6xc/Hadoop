package com.cloudwick.developer.pagecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageCountMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		Text t = new Text();
		Text t2 = new Text();
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String[] words = line.split(",");
		String user = words[0];
		String page = words[1];
		// for total count
		/*
		 * t.set(page); context.write(t,one);
		 */
		// for unique count
		t.set(page);
		t2.set(user);
		context.write(t, t2);

	}
}
