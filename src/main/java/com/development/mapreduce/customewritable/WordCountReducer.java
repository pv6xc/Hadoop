package com.development.mapreduce.customewritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<CustomWritableComparable,IntWritable,CustomWritableComparable,IntWritable>{

	@Override
	protected void reduce(CustomWritableComparable key, Iterable<IntWritable> value,
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
