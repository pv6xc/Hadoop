package com.developer.mapreduce.apachecustomwritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<ApacheCustomWritable,IntWritable,ApacheCustomWritable,IntWritable>{

	@Override
	protected void reduce(ApacheCustomWritable key, Iterable<IntWritable> value,
			Context context)
			throws IOException, InterruptedException {
		
		
		IntWritable temp = new IntWritable();
		int sum=0;
		for (IntWritable intWritable : value) {
			sum+=intWritable.get();
		}
		temp.set(sum);
		context.write(key,temp);
	}
	
}
