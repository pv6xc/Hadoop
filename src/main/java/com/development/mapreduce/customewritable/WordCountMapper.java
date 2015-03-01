package com.development.mapreduce.customewritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends
		Mapper<LongWritable, Text, CustomWritable, IntWritable> {


	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		Text word = new Text();
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String[] words = line.split(" ");
            context.write(new CustomWritable(words[0],words[1]),one);

	}

}
