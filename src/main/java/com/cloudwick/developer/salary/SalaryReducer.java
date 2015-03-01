package com.cloudwick.developer.salary;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable salarySumInt = new IntWritable();
		// salary sum code
		int salarySum = 0;
		for (IntWritable intWritable : value) {
			salarySum += intWritable.get();
		}
		salarySumInt.set(salarySum);
		context.write(key, salarySumInt);
	}

}
