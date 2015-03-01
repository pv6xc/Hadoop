package com.cloudwick.developer.avgwordcount;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, FloatWritable> {

	enum WordsCount{
		wordCount
	}
	/**
	 * Emit word and 1
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		Text word = new Text();
		FloatWritable one = new FloatWritable(1);
		String line = value.toString();
		String[] words = line.split(" ");
		for (String w:words) {
			word.set(w);
			context.getCounter(WordsCount.wordCount).increment(1);
			//context.write(word, one);
		}	
		
	}

}
