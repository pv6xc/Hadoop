package com.development.mapreduce.setupcleanup;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void run(
			Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.run(context);
	}

	@Override
	protected void cleanup(
			Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}

	@Override
	protected void setup(
			Context context)
			throws IOException, InterruptedException {

	}

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		Text word = new Text();
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String[] words = line.split(" ");
		for (int i = 0; i < words.length; i++) {
			word.set(words[0]);
			context.write(word, one);
		}	
		
		
		
	}

}
