package com.developer.mapreduce.apachecustomwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends
		Mapper<LongWritable, Text, ApacheCustomWritable, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String[] words = line.split(" ");
			context.write(new ApacheCustomWritable(words[0],words[6],words[3]), one);
	}

}
