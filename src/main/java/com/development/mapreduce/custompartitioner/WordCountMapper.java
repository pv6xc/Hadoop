package com.development.mapreduce.custompartitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
    enum JobCounter{
        counters
    }

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		Text word = new Text();
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		String[] words = line.split(" ");
		for (int i = 0; i < words.length; i++) {
            word.set(words[i]);
            context.write(word,one);
			context.getCounter(JobCounter.counters).increment(1);
		}	

		
	}

}
